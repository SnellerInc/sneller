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
		default:
			err = fmt.Errorf("unrecognized field %q (sym %d)", st.Get(sym), sym)
		}
		if err != nil {
			return nil, fmt.Errorf("blob.Compressed decode: %w", err)
		}
	}
	return c, nil
}

func (c *Compressed) encode(dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("type"))
	dst.WriteSymbol(st.Intern("blob.Compressed"))
	dst.BeginField(st.Intern("from"))
	encode(c.From, dst, st)
	dst.BeginField(st.Intern("trailer"))
	c.Trailer.Encode(dst, st)
	if c.etext != "" {
		dst.BeginField(st.Intern("etext"))
		dst.WriteString(c.etext)
	}
	dst.EndStruct()
}

// Split splits a compressed blobs
// into a list of blobs where all
// but the last blob in the list
// have a (decompressed) size of at least 'cut' bytes.
func (c *Compressed) Split(cut int) (*List, error) {
	l := &List{}
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
		newt := t.Slice(n, end)
		l.Contents = append(l.Contents, &Compressed{
			From:    c.From,
			Trailer: newt,
			etext:   fmt.Sprintf("%d-%d", newt.Blocks[0].Offset, newt.Offset),
		})
		n = end
	}
	return l, nil
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
	dd.dec.Trailer = c.Trailer
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
	cr.dec.Trailer = c.Trailer
	return cr, nil
}
