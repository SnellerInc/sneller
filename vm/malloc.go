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
	"math/bits"
	"sync"
	"sync/atomic"
	"unsafe"
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
	pageBits = 20
	pageSize = 1 << pageBits

	// number of bytes within reserved
	// mapping to make available via Malloc
	// (can be up to vmReserve - vmStart)
	vmUse = 1 << 29

	// total pages that can be used
	vmPages = vmUse >> pageBits

	// 64-bit words in allocation bitmap
	vmWords = vmPages / 64

	// total number of bytes to reserve
	vmReserve = 1 << 32

	// offset within vmReserve to set as vmm
	vmStart = vmReserve >> 1

	// PageSize is the granularity
	// of the allocations returned
	// by Malloc
	PageSize = pageSize
)

var (
	memlock sync.Mutex
	vmm     *[vmUse]byte
	vmbits  [vmWords]uint64

	// # pages in use; this may differ
	// slightly from the bitmap count
	// when we are freeing pages
	vminuse int64
)

// vmref is a (type, length) tuple
// referring to memory within the VMM
type vmref [2]uint32

// mem returns the memory to which v points
func (v vmref) mem() []byte {
	mem := vmm[v[0]:]
	return mem[:v[1]:v[1]]
}

// valid returns whether or not v
// is a valid reference; valid takes
// into account whether the memory
// pointed to by v has been reserved
// via a call to Malloc and has *not*
// yet been free'd
//
// NOTE: valid is fairly expensive to run;
// you should probably only use this in testing
// or with special build flags turned on
func (v vmref) valid() bool {
	if v[0] == 0 && v[1] == 0 {
		return true
	}
	if v[0] > vmUse || v[0]+v[1] > vmUse {
		return false
	}
	// the page that this points to
	// should have its allocated bit set
	pfn := v[0] >> pageBits
	bitmap := vmbits[pfn/64]
	bit := pfn % 64
	return bitmap&(1<<bit) != 0
}

// size returns the number of bytes
// that this vmref points to
func (v vmref) size() int { return int(v[1]) }

func init() {
	vmm = mapVM()
	guard(vmm[:vmUse])
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
//
// NOTE: Allocated will return true for
// a buffer allocated from Malloc even after
// it has been returned via Free. Allocated
// does *not* indicate whether the buffer is
// actually safe to access.
func Allocated(buf []byte) bool {
	_, ok := vmdispl(buf)
	return ok
}

// Malloc returns a new buffer suitable
// for passing to VM operations.
//
// If there is no VM memory available, Malloc panics.
func Malloc() []byte {
	// we loop while vminuse < vmPages because
	// we may be racing with Free locking groups
	// of pages in order to pass them to madvise(mem, MADV_FREE)
	for atomic.LoadInt64(&vminuse) < vmPages {
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
			atomic.AddInt64(&vminuse, 1)
			buf := vmm[((i*64)+bit)<<pageBits:]
			buf = buf[:pageSize:pageSize]
			unguard(buf) // if -tags=vmfence, unprotect this memory
			return buf
		}
	}
	panic("out of VM memory")
}

// PagesUsed returns the number of currently-active
// pages returned by Malloc that have not been
// deactivated with a call to Free.
func PagesUsed() int {
	return int(atomic.LoadInt64(&vminuse))
}

// should ordinarily return the same
// result as PagesUsed(), except when
// we are freeing pages
func vmPageBits() int {
	n := 0
	for i := range vmbits {
		n += bits.OnesCount64(atomic.LoadUint64(&vmbits[i]))
	}
	return n
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
	guard(buf) // if -tags=vmfence, protect this memory
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
			hintUnused(mem)
			atomic.StoreUint64(addr, 0)
			atomic.AddInt64(&vminuse, -1)
			return
		}
		if atomic.CompareAndSwapUint64(addr, mask, mask&^bit) {
			atomic.AddInt64(&vminuse, -1)
			return
		}
	}
}
