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

package blob

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// Compressed reads a blockfmt-formatted blob
type Compressed struct {
	// From specifies the source of the
	// compressed blob.
	From Interface
	// Trailer is the trailer that
	// describes how to unpack the
	// compressed contents of From.
	Trailer blockfmt.Trailer
	// etext is additional text used
	// to compute the ETag of the object
	// if the trailer has been manipulated
	// to point to different data (see compressedRange)
	etext string

	// interned ID; used for encoding
}

func extend(et, extra string) string {
	h := sha256.New()
	io.WriteString(h, et)
	io.WriteString(h, extra)
	return base64.URLEncoding.EncodeToString(h.Sum(nil))
}

// Stat implements Interface.Stat
func (c *Compressed) Stat() (*Info, error) {
	inner, err := c.From.Stat()
	if err != nil {
		return nil, err
	}
	etag := inner.ETag
	if c.etext != "" {
		etag = extend(etag, c.etext)
	}
	return &Info{
		ETag:         etag,
		Size:         c.Trailer.Offset - c.Trailer.Blocks[0].Offset,
		Align:        1 << c.Trailer.BlockShift,
		LastModified: inner.LastModified,
		Ephemeral:    inner.Ephemeral,
	}, nil
}

type decodeComp struct {
	parent *blobDecoder
	comp   *Compressed
}

func (d *decodeComp) getInterface() Interface {
	return d.comp
}

func (d *decodeComp) Init(*ion.Symtab) {
	d.comp = d.parent.compressed()
}

func (d *decodeComp) SetField(name string, body []byte) error {
	var err error
	switch name {
	case "from":
		d.comp.From, _, err = d.parent.decode(body)
	case "trailer":
		err = d.parent.td.Decode(body, &d.comp.Trailer)
	case "etext":
		d.comp.etext, _, err = ion.ReadString(body)
	case "skip":
		// ignore
	case "iid":
		var id int64
		id, _, err = ion.ReadInt(body)
		if err != nil {
			return err
		}
		expected := len(d.parent.interned) + 1
		if id != int64(expected) {
			return fmt.Errorf("unexpected iid %d (expected %d)", id, expected)
		}
		d.parent.interned = append(d.parent.interned, d.comp)
	default:
		return fmt.Errorf("unrecognized field")
	}

	return err
}

func (d *decodeComp) Finalize() error {
	return nil
}

func (c *Compressed) encode(be *blobEncoder, dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("type"))
	dst.WriteSymbol(st.Intern("blob.Compressed"))
	dst.BeginField(st.Intern("from"))
	be.encode(c.From, dst, st)
	dst.BeginField(st.Intern("trailer"))
	c.Trailer.Encode(dst, st)
	if c.etext != "" {
		dst.BeginField(st.Intern("etext"))
		dst.WriteString(c.etext)
	}
	if id, ok := be.id(c); ok {
		dst.BeginField(st.Intern("iid"))
		dst.WriteInt(int64(id))
	}
	dst.EndStruct()
}

// Split splits a compressed blobs
// into a list of blobs where all
// but the last blob in the list
// have a (decompressed) size of at least 'cut' bytes.
func (c *Compressed) Split(cut int) ([]CompressedPart, error) {
	var out []CompressedPart
	n := 0
	t := c.Trailer
	for n < len(t.Blocks) {
		b := &t.Blocks[n]
		end := n + 1
		size := b.Chunks << t.BlockShift
		for size < cut && end < len(t.Blocks) {
			newb := &t.Blocks[end]
			end++
			size += newb.Chunks << t.BlockShift
		}
		out = append(out, CompressedPart{
			StartBlock: n,
			EndBlock:   end,
			Parent:     c,
		})
		n = end
	}
	return out, nil
}

type compressedReader struct {
	io.ReadCloser
	dec blockfmt.Decoder
}

// WriteTo implements io.WriterTo
func (c *compressedReader) WriteTo(dst io.Writer) (int64, error) {
	return c.dec.Copy(dst, c.ReadCloser)
}

type decompressor struct {
	src io.ReadCloser
	dec blockfmt.Decoder
}

func (d *decompressor) Read(p []byte) (int, error) {
	return d.dec.Decompress(d.src, p)
}

func (d *decompressor) WriteTo(dst io.Writer) (int64, error) {
	return d.dec.Copy(dst, d.src)
}

func (d *decompressor) Close() error { return d.src.Close() }

func (c *Compressed) Decompressor() (io.ReadCloser, error) {
	start := c.Trailer.Blocks[0].Offset
	end := c.Trailer.Offset
	rd, err := c.From.Reader(start, end-start)
	if err != nil {
		return nil, err
	}
	dd := &decompressor{}
	dd.src = rd
	dd.dec.Set(&c.Trailer, len(c.Trailer.Blocks))
	return dd, nil
}

func (c *Compressed) Reader(start, size int64) (io.ReadCloser, error) {
	start += c.Trailer.Blocks[0].Offset
	rd, err := c.From.Reader(start, size)
	if err != nil {
		return nil, err
	}
	cr := &compressedReader{}
	cr.ReadCloser = rd
	cr.dec.Set(&c.Trailer, len(c.Trailer.Blocks))
	return cr, nil
}

// CompressedPart is a range of blocks
// within a Compressed blob.
type CompressedPart struct {
	Parent               *Compressed
	StartBlock, EndBlock int
}

// Decompressed returns the decompressed
// size of this compressed part.
func (c *CompressedPart) Decompressed() (bytes int64) {
	shift := c.Parent.Trailer.BlockShift
	blocks := c.Parent.Trailer.Blocks[c.StartBlock:c.EndBlock]
	for i := range blocks {
		bytes += int64(blocks[i].Chunks) << shift
	}
	return
}

// Reader implements Interface.Reader
func (c *CompressedPart) Reader(start, size int64) (io.ReadCloser, error) {
	start += c.Parent.Trailer.Blocks[c.StartBlock].Offset
	rd, err := c.Parent.From.Reader(start, size)
	if err != nil {
		return nil, err
	}
	cr := &compressedReader{}
	cr.ReadCloser = rd
	cr.dec.Set(&c.Parent.Trailer, c.EndBlock)
	return cr, nil
}

func (c *CompressedPart) Decompressor() (io.ReadCloser, error) {
	start := c.Parent.Trailer.Blocks[c.StartBlock].Offset
	end := c.Parent.Trailer.Offset
	if c.EndBlock < len(c.Parent.Trailer.Blocks) {
		end = c.Parent.Trailer.Blocks[c.EndBlock].Offset
	}
	rd, err := c.Parent.From.Reader(start, end-start)
	if err != nil {
		return nil, err
	}
	dd := &decompressor{}
	dd.src = rd
	dd.dec.Set(&c.Parent.Trailer, c.EndBlock)
	return dd, nil
}

// Stat implements Interface.Stat
func (c *CompressedPart) Stat() (*Info, error) {
	pinfo, err := c.Parent.Stat()
	if err != nil {
		return nil, err
	}
	info := new(Info)
	*info = *pinfo
	info.ETag = extend(info.ETag, fmt.Sprintf("%d-%d", c.StartBlock, c.EndBlock))
	start := c.Parent.Trailer.Blocks[c.StartBlock].Offset
	end := c.Parent.Trailer.Offset
	if c.EndBlock < len(c.Parent.Trailer.Blocks) {
		end = c.Parent.Trailer.Blocks[c.EndBlock].Offset
	}
	info.Size = end - start
	return info, nil
}

func (c *CompressedPart) encode(be *blobEncoder, dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("type"))
	dst.WriteSymbol(st.Intern("blob.CompressedPart"))
	dst.BeginField(st.Intern("start"))
	dst.WriteInt(int64(c.StartBlock))
	dst.BeginField(st.Intern("end"))
	dst.WriteInt(int64(c.EndBlock))
	if id, ok := be.id(c.Parent); ok {
		dst.BeginField(st.Intern("parent-id"))
		dst.WriteInt(int64(id))
	} else {
		be.intern(c.Parent)
		dst.BeginField(st.Intern("parent"))
		c.Parent.encode(be, dst, st)
	}
	dst.EndStruct()
}

type decodeCPart struct {
	st     *ion.Symtab
	parent *blobDecoder
	comp   *CompressedPart
}

func (d *decodeCPart) getInterface() Interface {
	return d.comp
}

func (d *decodeCPart) Init(st *ion.Symtab) {
	d.st = st
	d.comp = new(CompressedPart)
}

func (d *decodeCPart) SetField(name string, body []byte) error {
	var err error
	var n int64
	switch name {
	case "start":
		n, _, err = ion.ReadInt(body)
		d.comp.StartBlock = int(n)
	case "end":
		n, _, err = ion.ReadInt(body)
		d.comp.EndBlock = int(n)
	case "parent-id":
		n, _, err = ion.ReadInt(body)
		if err == nil {
			if idx := n - 1; idx >= 0 && int(idx) < len(d.parent.interned) {
				d.comp.Parent = d.parent.interned[idx]
			} else {
				err = fmt.Errorf("bad parent-id %d (of %d)", n, len(d.parent.interned))
			}
		}
	case "parent":
		dec := decodeComp{parent: d.parent}

		setitem := func(typename string) error {
			if typename != "blob.Compressed" {
				return fmt.Errorf("unexpected parent blob type %q", typename)
			}

			dec.Init(d.st)
			return nil
		}

		_, err := ion.UnpackTypedStruct(d.st, body, setitem, dec.SetField)
		if err != nil {
			return err
		}

		err = dec.Finalize()
		if err != nil {
			return err
		}

		d.comp.Parent = dec.comp
		return nil
	default:
		return fmt.Errorf("unrecognized field")
	}

	return err
}

func (d *decodeCPart) Finalize() error {
	if d.comp.StartBlock > d.comp.EndBlock {
		return fmt.Errorf("blob.CompressedPart decode: start %d > end %d", d.comp.StartBlock, d.comp.EndBlock)
	}
	if d.comp.Parent == nil {
		return fmt.Errorf("blob.CompressedPart decode: missing parent or parent-id")
	}
	if d.comp.EndBlock > len(d.comp.Parent.Trailer.Blocks) {
		return fmt.Errorf("blob.CompressedPart end block %d > len(parent.Blocks)=%d", d.comp.EndBlock, len(d.comp.Parent.Trailer.Blocks))
	}

	return nil
}
