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

package sneller

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tenant/dcache"
	"github.com/SnellerInc/sneller/vm"
	"github.com/dchest/siphash"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

var CanVMOpen = false

// CacheLimit defines a limit such that blob
// segments will not be cached if the total scan
// size of a request in bytes exceeds the limit.
var CacheLimit = memTotal / 2

var onebuf [8]byte

func init() {
	binary.LittleEndian.PutUint64(onebuf[:], 1)
}

// TenantEnv implements plan.Decoder for use
// with snellerd in tenant mode. It also
// implements plan.Env, though must have the
// embedded FSEnv initialized in order to be
// used as such.
type TenantEnv struct {
	*FSEnv
}

type TenantRunner struct {
	Events *os.File
	Cache  *dcache.Cache
}

func (r *TenantRunner) Post() {
	if r.Events != nil {
		r.Events.Write(onebuf[:])
	}
}

func (r *TenantRunner) Run(dst vm.QuerySink, in *plan.Input, ep *plan.ExecParams) error {
	// TODO: this should be reimplemented in terms
	// of plan.FSRunner
	ctx := ep.Context
	if ctx == nil {
		ctx = context.Background()
	}
	if !CanVMOpen {
		panic("shouldn't have called Run")
	}
	segs := make([]dcache.Segment, 0, len(in.Blocks))
	for i := range in.Blocks {
		seg := &tenantSegment{
			fs:     ep.FS,
			desc:   in.Descs[in.Blocks[i].Index],
			block:  in.Blocks[i].Offset,
			fields: in.Fields,
		}
		segs = append(segs, seg)
	}
	if len(segs) == 0 {
		return nil
	}
	var flags dcache.Flag
	if CacheLimit > 0 && in.CompressedSize() > CacheLimit {
		flags = dcache.FlagNoFill
	}
	tbl := r.Cache.MultiTable(ctx, segs, flags)
	err := tbl.WriteChunks(dst, ep.Parallel)
	ep.Stats.Observe(tbl)
	return err
}

// tenantSegment implements dcache.Segment
type tenantSegment struct {
	fs     fs.FS
	desc   blockfmt.Descriptor
	block  int
	fields []string
}

// merge two sorted slices
func merge[T constraints.Ordered](dst, src []T) []T {
	if slices.Equal(dst, src) {
		return dst
	}

	var out []T
	j := 0
	for i := 0; i < len(dst); i++ {
		if j >= len(src) {
			out = append(out, dst[i:]...)
			break
		}
		if dst[i] == src[j] {
			out = append(out, dst[i])
			j++
		} else if dst[i] < src[j] {
			out = append(out, dst[i])
		} else {
			out = append(out, src[j])
			j++
			i--
		}
	}
	out = append(out, src[j:]...)
	return out
}

func (s *tenantSegment) Merge(other dcache.Segment) {
	o := other.(*tenantSegment)
	all := s.fields == nil || o.fields == nil
	if all {
		s.fields = nil
	} else {
		s.fields = merge(s.fields, o.fields)
	}
}

// Size is currently the blob size
func (s *tenantSegment) Size() int64 {
	size := int64(0)
	start, end := s.desc.Trailer.BlockRange(s.block)
	size += end - start
	return size
}

// ETag implements dcache.Segment.ETag
func (s *tenantSegment) ETag() string {
	// we're hashing the etag+block together
	// so that we get an even dispersion of
	// bits for the top byte (base64-encoded)
	// which we use as the directory for the
	// rest of the cache contents
	//
	// this avoids an issue for ETags that begin
	// with deterministic sequences of characters
	// (like S3 ETags beginning with '"')
	// causing the first level of cache directory
	// indirection to become entirely useless
	const (
		k0 = 0x9f17c3fd5efd3ce4
		k1 = 0xdbf1ba5f07eee2c0
	)
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s-%d", s.desc.ETag, s.block)
	lo, hi := siphash.Hash128(k0, k1, buf.Bytes())
	mem := buf.Bytes()[:0]
	mem = binary.LittleEndian.AppendUint64(mem, lo)
	mem = binary.LittleEndian.AppendUint64(mem, hi)
	return base64.URLEncoding.EncodeToString(mem)
}

// Read implements dcache.Segment.Open
func (s *tenantSegment) Open() (io.ReadCloser, error) {
	f, err := s.fs.Open(s.desc.Path)
	if err != nil {
		return nil, err
	}
	start, end := s.desc.Trailer.BlockRange(s.block)
	return plan.SectionReader(f, start, end-start)
}

func (s *tenantSegment) Ephemeral() bool {
	return s.desc.Size < db.DefaultMinMerge
}

func vmMalloc(size int) []byte {
	if size > vm.PageSize {
		panic("cannot allocate page with size > vm.PageSize")
	}
	return vm.Malloc()[:size]
}

// Decode implements dcache.Segment.Decode
func (s *tenantSegment) Decode(dst io.Writer, src []byte) error {
	var dec blockfmt.Decoder
	dec.Malloc = vmMalloc
	dec.Free = vm.Free
	dec.Fields = s.fields
	dec.Set(&s.desc.Trailer)
	_, err := dec.CopyBytes(dst, src)
	return err
}
