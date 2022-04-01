// Copyright (C) 2022 Sneller, Inc.
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"fmt"
	"io"
	"math/bits"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/SnellerInc/sneller/ion"
)

// VM "memory" aka VMM
//
// We reserve 4GiB of memory at start-up
// that is dedicated to VM operations.
// Restricting the VM to operating on
// a fixed 4GiB region lets us use 32-bit
// "absolute" addresses in the VM even though
// the VM may reference data in different buffers
// (i.e. one from io.Writer.Write, the scratch buffer, etc.)

const (
	pageBits  = 20
	pageSize  = 1 << pageBits
	vmUse     = 1 << 29
	vmPages   = vmUse >> pageBits
	vmWords   = vmPages / 64
	vmReserve = 1 << 32

	// PageSize is the granularity
	// of the allocations returned
	// by Malloc
	PageSize = pageSize
)

var (
	memlock sync.Mutex
	vmm     *[vmReserve]byte
	vmbits  [vmWords]uint64
)

// vmref is a (type, length) tuple
// referring to memory within the VMM
type vmref [2]uint32

func (v vmref) mem() []byte {
	mem := vmm[v[0]:]
	return mem[:v[1]:v[1]]
}

func init() {
	buf, err := syscall.Mmap(0, 0, vmReserve, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS)
	if err != nil {
		panic("couldn't map vmm region: " + err.Error())
	}
	if vmUse < vmReserve {
		err = syscall.Mprotect(buf[vmUse:], syscall.PROT_NONE)
		if err != nil {
			panic("couldn't map unused vmm region as PROT_NONE: " + err.Error())
		}
	}
	vmm = (*[vmReserve]byte)(buf)
}

func vmbase() uintptr {
	return uintptr(unsafe.Pointer(vmm))
}

func vmend() uintptr {
	return vmbase() + vmUse
}

// vmdispl returns the displacement
// of the base of buf relative to
// the vmm base, or (0, false) if
// the buffer does not have a valid vmm displacement
func vmdispl(buf []byte) (uint32, bool) {
	p := uintptr(unsafe.Pointer(&buf[0]))
	if p < vmbase() || p >= vmend() {
		return 0, false
	}
	return uint32(p - vmbase()), true
}

// Allocated returns true if buf
// was returned from Malloc, or false otherwise.
func Allocated(buf []byte) bool {
	_, ok := vmdispl(buf)
	return ok
}

// Malloc returns a new buffer suitable
// for passing to VM operations.
func Malloc() []byte {
	for i := 0; i < vmWords; i++ {
		addr := &vmbits[i]
		mask := atomic.LoadUint64(addr)
		avail := ^mask
		if avail == uint64(0) {
			continue
		}
		bit := bits.TrailingZeros64(avail)
		if !atomic.CompareAndSwapUint64(addr, mask, mask|(uint64(1)<<bit)) {
			i--
			continue
		}
		buf := vmm[((i*64)+bit)<<pageBits:]
		buf = buf[:pageSize:pageSize]
		return buf
	}
	return nil
}

func PagesUsed() int {
	n := 0
	for i := range vmbits {
		n += bits.OnesCount64(vmbits[i])
	}
	return n
}

// needsVMM determines if the target io.Writer
// *really* needs a malloc'd input buffer
func needsVMM(dst io.Writer) bool {
	if s, ok := dst.(*sink); ok {
		dst = s.dst
	}
	_, ok := dst.(RowConsumer)
	if ok {
		return ok
	}
	_, ok = dst.(*RowSplitter)
	return ok
}

// Free frees a buffer that was returned
// by Malloc so that it can be re-used.
// The caller may not use the contents
// of buf after it has called Free.
func Free(buf []byte) {
	buf = buf[:pageSize]
	p := uintptr(unsafe.Pointer(&buf[0]))
	if p < vmbase() || p >= vmend() {
		panic("bad pointer passed to Free()")
	}
	pfn := (p - vmbase()) >> pageBits
	bit := uint64(1) << (pfn % 64)
	addr := &vmbits[pfn/64]
	for {
		mask := atomic.LoadUint64(addr)
		if mask&bit == 0 {
			panic("double-vm.Free()")
		}
		// if we are about to set a whole region to zero,
		// then madvise the whole thing to being unused
		// once we have locked all the page bits
		if mask == bit && atomic.CompareAndSwapUint64(addr, mask, ^uint64(0)) {
			width := 64 << pageBits
			base := (64 * (pfn / 64)) << pageBits
			mem := vmm[base:]
			mem = mem[:width:width]
			err := syscall.Madvise(mem, 8) // MADV_FREE
			if err != nil {
				panic("madvise: " + err.Error())
			}
			atomic.StoreUint64(addr, 0)
			return
		}
		if atomic.CompareAndSwapUint64(addr, mask, mask&^bit) {
			return
		}
	}
}

func safeWrite(dst io.Writer, src *ion.Buffer) error {
	if !needsVMM(dst) {
		_, err := dst.Write(src.Bytes())
		return err
	}
	if src.Size() < PageSize {
		tmp := Malloc()
		_, err := dst.Write(tmp[:copy(tmp, src.Bytes())])
		Free(tmp)
		return err
	}
	// TODO: copy segments,
	// taking care to reproduce the
	// leading symbol table when appropriate
	return fmt.Errorf("size %d > PageSize %d", src.Size(), PageSize)
}
