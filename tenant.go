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
	parent *TenantEnv
	inner  plan.TableHandle
}

func (t *TenantEnv) Stat(tbl expr.Node, h *plan.Hints) (plan.TableHandle, error) {
	if t.FSEnv == nil {
		panic("plan.TenantEnv: cannot call Stat without FSEnv set")
	}
	th, err := t.FSEnv.Stat(tbl, h)
	if err != nil {
		return nil, err
	}
	return &TenantHandle{parent: t, inner: th}, nil
}

func (t *TenantEnv) DecodeHandle(st *ion.Symtab, buf []byte) (plan.TableHandle, error) {
	h := new(FilterHandle)
	if err := h.Decode(st, buf); err != nil {
		return nil, err
	}
	return &TenantHandle{parent: t, inner: h}, nil
}

var _ plan.SubtableDecoder = (*TenantEnv)(nil)

// DecodeSubtables implements plan.SubtableDecoder.
func (t *TenantEnv) DecodeSubtables(st *ion.Symtab, buf []byte) (plan.Subtables, error) {
	thfn := func(blobs []blob.Interface, hint *plan.Hints) plan.TableHandle {
		h := &FilterHandle{
			Blobs:     &blob.List{Contents: blobs},
			Fields:    hint.Fields,
			AllFields: hint.AllFields,
			Expr:      hint.Filter,
		}
		return &TenantHandle{parent: t, inner: h}
	}
	return DecodeSubtables(st, buf, thfn)
}

var _ plan.UploaderDecoder = (*TenantEnv)(nil)

// DecodeUploader implements plan.UploaderDecoder.
func (t *TenantEnv) DecodeUploader(st *ion.Symtab, buf []byte) (plan.UploadFS, error) {
	if t.Local {
		return db.DecodeDirFS(st, buf)
	}
	return db.DecodeS3FS(st, buf)
}

func (t *TenantEnv) Post() {
	if t.Events != nil {
		t.Events.Write(onebuf[:])
	}
}

func (h *TenantHandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	return h.inner.Encode(dst, st)
}

func (h *TenantHandle) Open(ctx context.Context) (vm.Table, error) {
	fh := h.inner.(*FilterHandle)
	lst := fh.Blobs
	if !CanVMOpen {
		panic("shouldn't have called tenantHandle.Open()")
	}
	segs := make([]dcache.Segment, 0, len(lst.Contents))
	flt, _ := fh.CompileFilter()
	var size int64
	for i := range lst.Contents {
		if h.parent.HTTPClient != nil {
			blob.Use(lst.Contents[i], h.parent.HTTPClient)
		}
		b := lst.Contents[i]
		if pc, ok := b.(*blob.CompressedPart); ok && flt != nil {
			if scan := maxscan(pc, flt); scan == 0 {
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
		parent: h.parent,
		inner:  h.inner.(*FilterHandle).Filter(e),
	}
}

type emptyTable struct{}

func (emptyTable) Chunks() int { return 0 }

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

// Decode implements dcache.Segment.Decode
func (b *blobSegment) Decode(dst io.Writer, src []byte) error {
	if c, ok := b.blob.(*blob.CompressedPart); ok {
		// compressed: do decoding
		var dec blockfmt.Decoder
		dec.Fields = b.fieldList()
		dec.Set(c.Parent.Trailer, c.EndBlock)
		_, err := dec.CopyBytes(dst, src)
		return err
	}
	if c, ok := b.blob.(*blob.Compressed); ok {
		var dec blockfmt.Decoder
		dec.Set(c.Trailer, len(c.Trailer.Blocks))
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
