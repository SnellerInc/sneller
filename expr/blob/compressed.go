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
	Trailer *blockfmt.Trailer
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

func (d *blobDecoder) decodeComp(fields []byte) (*Compressed, error) {
	c := d.compressed()
	var err error
	var sym ion.Symbol
	st := d.td.Symbols
	for len(fields) > 0 {
		sym, fields, err = ion.ReadLabel(fields)
		if err != nil {
			return nil, err
		}
		switch st.Get(sym) {
		case "from":
			c.From, fields, err = d.decode(fields)
		case "trailer":
			c.Trailer, err = d.td.Decode(fields)
			if err == nil {
				fields = fields[ion.SizeOf(fields):]
			}
		case "etext":
			c.etext, fields, err = ion.ReadString(fields)
		case "skip":
			// ignore
			_, fields, err = ion.ReadBytes(fields)
		case "iid":
			var id int64
			id, fields, err = ion.ReadInt(fields)
			if err != nil {
				return nil, err
			}
			if id != int64(len(d.interned)+1) {
				return nil, fmt.Errorf("blob.Compressed decode: unexpected iid %d (expected %d)", id, len(d.interned)+1)
			}
			d.interned = append(d.interned, c)
		default:
			err = fmt.Errorf("unrecognized field %q (sym %d)", st.Get(sym), sym)
		}
		if err != nil {
			return nil, fmt.Errorf("blob.Compressed decode: %w", err)
		}
	}
	return c, nil
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
	dd.dec.Set(c.Trailer, len(c.Trailer.Blocks))
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
	cr.dec.Set(c.Trailer, len(c.Trailer.Blocks))
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
	cr.dec.Set(c.Parent.Trailer, c.EndBlock)
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
	dd.dec.Set(c.Parent.Trailer, c.EndBlock)
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

func (d *blobDecoder) decodeCPart(fields []byte) (*CompressedPart, error) {
	out := new(CompressedPart)
	st := d.td.Symbols
	var err error
	var sym ion.Symbol
	for len(fields) > 0 {
		sym, fields, err = ion.ReadLabel(fields)
		if err != nil {
			return nil, err
		}
		var n int64
		switch st.Get(sym) {
		case "start":
			n, fields, err = ion.ReadInt(fields)
			out.StartBlock = int(n)
		case "end":
			n, fields, err = ion.ReadInt(fields)
			out.EndBlock = int(n)
		case "parent-id":
			n, fields, err = ion.ReadInt(fields)
			if idx := n - 1; idx >= 0 && int(idx) < len(d.interned) {
				out.Parent = d.interned[idx]
			} else {
				err = fmt.Errorf("bad parent-id %d (of %d)", n, len(d.interned))
			}
		case "parent":
			var p *Compressed
			var body []byte
			body, fields = ion.Contents(fields)
			sym, body, err = ion.ReadLabel(body)
			if err != nil {
				break
			}
			if st.Get(sym) != "type" {
				err = fmt.Errorf("unexpected field %q", st.Get(sym))
				break
			}
			sym, body, err = ion.ReadSymbol(body)
			if err != nil {
				err = fmt.Errorf("reading type symbol: %w", err)
				break
			}
			if st.Get(sym) != "blob.Compressed" {
				err = fmt.Errorf("unexpected parent blob type %q", st.Get(sym))
				break
			}
			p, err = d.decodeComp(body)
			out.Parent = p
		default:
			err = fmt.Errorf("unrecognized field %q", st.Get(sym))
		}
		if err != nil {
			return nil, fmt.Errorf("blob.CompressedPart decode: %w", err)
		}
	}
	if out.StartBlock > out.EndBlock {
		return nil, fmt.Errorf("blob.CompressedPart decode: start %d > end %d", out.StartBlock, out.EndBlock)
	}
	if out.Parent == nil {
		return nil, fmt.Errorf("blob.CompressedPart decode: missing parent or parent-id")
	}
	if out.EndBlock > len(out.Parent.Trailer.Blocks) {
		return nil, fmt.Errorf("blob.CompressedPart end block %d > len(parent.Blocks)=%d", out.EndBlock, len(out.Parent.Trailer.Blocks))
	}
	return out, nil
}
