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
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tenant/dcache"
	"github.com/SnellerInc/sneller/vm"
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
	HTTPClient *http.Client
	Events     *os.File
	Cache      *dcache.Cache

	// Local causes DecodeUploader to return a
	// *db.DirFS instead of a *db.S3FS. This is
	// intended to be used for testing.
	Local bool
}

type TenantHandle struct {
	*FilterHandle // share Size(), Encode()
	parent        *TenantEnv
}

var (
	_ plan.SplitHandle     = &TenantHandle{}
	_ plan.PartitionHandle = &TenantHandle{}
)

func (t *TenantEnv) Stat(tbl expr.Node, h *plan.Hints) (plan.TableHandle, error) {
	if t.FSEnv == nil {
		panic("plan.TenantEnv: cannot call Stat without FSEnv set")
	}
	th, err := t.FSEnv.Stat(tbl, h)
	if err != nil {
		return nil, err
	}
	return &TenantHandle{parent: t, FilterHandle: th.(*FilterHandle)}, nil
}

func (t *TenantEnv) DecodeHandle(d ion.Datum) (plan.TableHandle, error) {
	h := new(FilterHandle)
	if err := h.Decode(d); err != nil {
		return nil, err
	}
	return &TenantHandle{parent: t, FilterHandle: h}, nil
}

var _ plan.UploaderDecoder = (*TenantEnv)(nil)

// DecodeUploader implements plan.UploaderDecoder.
func (t *TenantEnv) DecodeUploader(d ion.Datum) (plan.UploadFS, error) {
	if t.Local {
		return db.DecodeDirFS(d)
	}
	return db.DecodeS3FS(d)
}

func (t *TenantEnv) Post() {
	if t.Events != nil {
		t.Events.Write(onebuf[:])
	}
}

func (h *TenantHandle) Split() (plan.Subtables, error) {
	return h.Splitter.split(h)
}

func (h *TenantHandle) Open(ctx context.Context) (vm.Table, error) {
	fh := h.FilterHandle
	lst := fh.Blobs
	if !CanVMOpen {
		panic("shouldn't have called tenantHandle.Open()")
	}
	filt, _ := fh.CompileFilter()
	segs := make([]dcache.Segment, 0, len(lst.Contents))
	var size int64
	for i := range lst.Contents {
		if h.parent.HTTPClient != nil {
			blob.Use(lst.Contents[i], h.parent.HTTPClient)
		}
		b := lst.Contents[i]
		if pc, ok := b.(*blob.CompressedPart); ok && filt != nil {
			if !filt.Overlaps(&pc.Parent.Trailer.Sparse, pc.StartBlock, pc.EndBlock) {
				continue
			}
		}
		seg := &blobSegment{
			fields:    fh.Fields,
			allFields: fh.AllFields,
			blob:      b,
		}
		// make sure info can be populated successfully
		s, err := seg.stat()
		if err != nil {
			return nil, err
		}
		segs = append(segs, seg)
		size += s.Size
	}
	if len(segs) == 0 {
		return emptyTable{}, nil
	}
	var flags dcache.Flag
	if CacheLimit > 0 && size > CacheLimit {
		flags = dcache.FlagNoFill
	}
	return h.parent.Cache.MultiTable(ctx, segs, flags), nil
}

func (h *TenantHandle) Filter(e expr.Node) plan.TableHandle {
	return &TenantHandle{
		parent:       h.parent,
		FilterHandle: h.FilterHandle.Filter(e).(*FilterHandle),
	}
}

func (h *TenantHandle) SplitBy(parts []string) ([]plan.TablePart, error) {
	type part struct {
		values []ion.Datum
		blobs  *blob.List
	}
	var st ion.Symtab // not really used
	var buf ion.Buffer

	// since partition constants are always strings or numbers,
	// we can use their concatenated ion representation as the
	// key for splitting them into the appropriate tables
	putkey := func(t *blockfmt.Trailer, dst *ion.Buffer) error {
		dst.Reset()
		for _, part := range parts {
			d, ok := t.Sparse.Const(part)
			if !ok {
				return fmt.Errorf("trailer missing part %q", part)
			}
			d.Encode(dst, &st)
		}
		return nil
	}

	partset := make(map[string]*part)

	add := func(b blob.Interface, t *blockfmt.Trailer) error {
		err := putkey(t, &buf)
		if err != nil {
			return err
		}
		if p := partset[string(buf.Bytes())]; p != nil {
			p.blobs.Contents = append(p.blobs.Contents, b)
			return nil
		}
		var values []ion.Datum
		for _, part := range parts {
			dat, _ := t.Sparse.Const(part)
			values = append(values, dat)
		}
		partset[string(buf.Bytes())] = &part{
			blobs:  &blob.List{Contents: []blob.Interface{b}},
			values: values,
		}
		return nil
	}

	for i := range h.Blobs.Contents {
		var err error
		switch b := h.Blobs.Contents[i].(type) {
		case *blob.Compressed:
			err = add(b, &b.Trailer)
		case *blob.CompressedPart:
			err = add(b, &b.Parent.Trailer)
		default:
			err = fmt.Errorf("cannot split on blob type %T", b)
		}
		if err != nil {
			return nil, err
		}
	}
	ret := make([]plan.TablePart, 0, len(partset))
	for _, v := range partset {
		fh := new(FilterHandle)
		*fh = *h.FilterHandle
		fh.Blobs = v.blobs
		// not safe to copy:
		fh.compiled = blockfmt.Filter{}
		ret = append(ret, plan.TablePart{
			Handle: &TenantHandle{
				parent:       h.parent,
				FilterHandle: fh,
			},
			Parts: v.values,
		})
	}
	return ret, nil
}

type emptyTable struct{}

func (emptyTable) WriteChunks(dst vm.QuerySink, parallel int) error {
	w, err := dst.Open()
	if err != nil {
		return err
	}
	return w.Close()
}

// blobSegment implements dcache.Segment
type blobSegment struct {
	blob      blob.Interface
	info      *blob.Info
	fields    []string
	allFields bool
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

// fieldList produces the canonical field list
// for use in blockfmt.Decoder.Fields (see the
// doc comment about the difference btw a nil
// slice versus a zero-length slice)
func (b *blobSegment) fieldList() []string {
	if b.allFields {
		return nil
	}
	ret := b.fields
	if ret == nil {
		ret = []string{}
	}
	return ret
}

func (b *blobSegment) Merge(other dcache.Segment) {
	o := other.(*blobSegment)
	b.allFields = b.allFields || o.allFields
	if b.allFields {
		b.fields = nil
		return
	}
	b.fields = merge(b.fields, o.fields)
}

func (b *blobSegment) stat() (*blob.Info, error) {
	if b.info != nil {
		return b.info, nil
	}
	info, err := b.blob.Stat()
	if err != nil {
		return nil, err
	}
	b.info = info
	return info, nil
}

// Size is currently the blob size
func (b *blobSegment) Size() int64 { return b.info.Size }

// ETag implements dcache.Segment.ETag
func (b *blobSegment) ETag() string { return b.info.ETag }

// Read implements dcache.Segment.Open
func (b *blobSegment) Open() (io.ReadCloser, error) {
	return b.blob.Reader(0, b.info.Size)
}

func (b *blobSegment) Ephemeral() bool { return b.info.Ephemeral }

func vmMalloc(size int) []byte {
	if size > vm.PageSize {
		panic("cannot allocate page with size > vm.PageSize")
	}
	return vm.Malloc()[:size]
}

// Decode implements dcache.Segment.Decode
func (b *blobSegment) Decode(dst io.Writer, src []byte) error {
	if c, ok := b.blob.(*blob.CompressedPart); ok {
		// compressed: do decoding
		var dec blockfmt.Decoder
		dec.Malloc = vmMalloc
		dec.Free = vm.Free
		dec.Fields = b.fieldList()
		dec.Set(&c.Parent.Trailer, c.EndBlock)
		_, err := dec.CopyBytes(dst, src)
		return err
	}
	if c, ok := b.blob.(*blob.Compressed); ok {
		var dec blockfmt.Decoder
		dec.Malloc = vmMalloc
		dec.Free = vm.Free
		dec.Set(&c.Trailer, len(c.Trailer.Blocks))
		dec.Fields = b.fieldList()
		_, err := dec.CopyBytes(dst, src)
		return err
	}
	// default: just write the segments directly
	for off := int64(0); off < b.info.Size; off += int64(b.info.Align) {
		mem := src[off:]
		if len(mem) > b.info.Align {
			mem = mem[:b.info.Align]
		}
		_, err := dst.Write(mem)
		if err != nil {
			return err
		}
	}
	return nil
}
