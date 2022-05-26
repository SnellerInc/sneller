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

package plan

import (
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
	"sync"

	"github.com/SnellerInc/sneller/compr"
	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/vm"
)

// uploadSink is a vm.QuerySink that uploads
// data to a single packfile
type uploadSink struct {
	mw    blockfmt.MultiWriter
	store blockfmt.UploadFS
	name  string
	dst   vm.QuerySink
}

// uploadStream is the io.WriteCloser
// returned from uploadSink.Open()
type uploadStream struct {
	ion.Chunker
}

func (up *uploadSink) Open() (io.WriteCloser, error) {
	w, err := up.mw.Open()
	if err != nil {
		return nil, err
	}
	ret := &uploadStream{}
	ret.W = w
	ret.Align = up.mw.InputAlign
	ret.RangeAlign = 100 * ret.Align
	return ret, nil
}

func (up *uploadStream) Close() error {
	err := up.Flush()
	err2 := up.W.(io.Closer).Close()
	if err == nil {
		err = err2
	}
	return err
}

func statdesc(ofs blockfmt.UploadFS, path string, up blockfmt.Uploader, into *blockfmt.Descriptor) error {
	into.Path = path
	into.Size = up.Size()
	type etagger interface {
		ETag() string
	}
	info, err := fs.Stat(ofs, path)
	if err != nil {
		return err
	}
	etag, err := ofs.ETag(path, info)
	if err != nil {
		return err
	}
	if et, ok := up.(etagger); ok {
		into.ETag = et.ETag()
	} else {
		into.ETag = etag
	}
	into.LastModified = date.FromTime(info.ModTime())
	return nil
}

func (up *uploadSink) Close() error {
	err := up.mw.Close()
	if err != nil {
		return err
	}

	var desc blockfmt.Descriptor
	desc.Trailer = &up.mw.Trailer
	err = statdesc(up.store, up.name, up.mw.Output, &desc)
	if err != nil {
		return err
	}
	// fast-path: don't serialize the descriptor
	// if we don't need to
	if is, ok := up.dst.(*indexSink); ok {
		is.rawAppend(&desc)
		return nil
	}

	// write the descriptor
	// as a single output row
	var buf ion.Buffer
	var st ion.Symtab
	blockfmt.WriteDescriptor(&buf, &st, &desc)
	tail := buf.Bytes()
	buf.Set(nil)
	st.Marshal(&buf, true)
	w, err := up.dst.Open()
	if err != nil {
		return err
	}
	_, err = w.Write(append(buf.Bytes(), tail...))
	if err != nil {
		w.Close()
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}
	return up.dst.Close()
}

// OutputPart is a nonterminal plan node
// that produces one blockfmt.Descriptor row
// for each thread of execution that points
// to an uploaded file containing all the data
// written into this node.
type OutputPart struct {
	Nonterminal
	Basename string
	Store    UploadFS
}

func uuid() string {
	var buf [16]byte
	_, err := rand.Read(buf[:])
	if err != nil {
		// crypto random source is busted?
		panic(err)
	}
	// remove the trailing padding; it is deterministic
	return strings.TrimSuffix(base32.StdEncoding.EncodeToString(buf[:]), "======")
}

func (o *OutputPart) exec(dst vm.QuerySink, parallel int, stat *ExecStats, rw TableRewrite) error {
	if o.Basename == "" {
		return fmt.Errorf("OutputPart: basename not set")
	} else if o.Store == nil {
		return fmt.Errorf("OutputPart: store not set")
	}
	name := path.Join(o.Basename, "packed-"+uuid())
	up, err := o.Store.Create(name)
	if err != nil {
		return err
	}
	us := &uploadSink{
		store: o.Store,
		name:  name,
		dst:   dst,
	}
	us.mw.Output = up
	us.mw.Comp = compr.Compression("zstd")
	us.mw.InputAlign = 1 << 20
	return o.From.exec(us, parallel, stat, rw)
}

func (o *OutputPart) encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.BeginStruct(-1)
	settype("outpart", dst, st)
	dst.BeginField(st.Intern("basename"))
	dst.WriteString(o.Basename)
	dst.BeginField(st.Intern("store"))
	if err := o.Store.Encode(dst, st); err != nil {
		return err
	}
	dst.EndStruct()
	return nil
}

func (o *OutputPart) setfield(d Decoder, name string, st *ion.Symtab, buf []byte) error {
	switch name {
	case "basename":
		basename, _, err := ion.ReadString(buf)
		if err != nil {
			return err
		}
		o.Basename = basename
	case "store":
		up, ok := d.(UploaderDecoder)
		if !ok {
			return fmt.Errorf("Decoder doesn't support UploaderDecoder: %T", d)
		}
		store, err := up.DecodeUploader(st, buf)
		if err != nil {
			return err
		}
		o.Store = store
	}
	return nil
}

func (o *OutputPart) String() string {
	return "OUTPUT PART " + o.Basename
}

// OutputIndex is a nonterminal plan node
// that accepts rows from OutputPart and collects
// them into an Index object. OutputIndex writes
// one output row containing the autogenerated
// table name.
type OutputIndex struct {
	Nonterminal
	Table    *expr.Path
	Basename string
	Store    UploadFS
	Key      *blockfmt.Key
}

// indexSink is a vm.QuerySink that collects
// the results of OutputPart and writes an Index
// on the final Close and returns the autogenerated
// table name as a single output record
type indexSink struct {
	parent *OutputIndex
	lock   sync.Mutex
	tbl    *expr.Path
	idx    *blockfmt.Index
	dst    vm.QuerySink
	closed bool
}

type indexWriter struct {
	syms   ion.Symtab
	parent *indexSink
}

func (i *indexSink) rawAppend(desc *blockfmt.Descriptor) {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.idx.Inline = append(i.idx.Inline, *desc)
}

func (i *indexWriter) Write(p []byte) (int, error) {
	var err error
	n := len(p)
	if ion.IsBVM(p) || ion.TypeOf(p) == ion.AnnotationType {
		p, err = i.syms.Unmarshal(p)
		if err != nil {
			return 0, err
		}
	}
	for len(p) > 0 {
		var desc *blockfmt.Descriptor
		desc, p, err = blockfmt.ReadDescriptor(p, &i.syms)
		if err != nil {
			return n - len(p), err
		}
		i.parent.rawAppend(desc)
	}
	return n, nil
}

func (i *indexWriter) Close() error { return nil }

func (is *indexSink) Open() (io.WriteCloser, error) {
	return &indexWriter{
		parent: is,
	}, nil
}

func (is *indexSink) Close() error {
	if is.closed {
		return nil
	}
	is.closed = true
	idxmem, err := blockfmt.Sign(is.parent.Key, is.idx)
	if err != nil {
		return err
	}
	idxpath := db.IndexPath(is.tbl.First, is.tbl.Rest.(*expr.Dot).Field)
	_, err = is.parent.Store.WriteFile(idxpath, idxmem)
	if err != nil {
		return err
	}
	var buf ion.Buffer
	var st ion.Symtab
	tabsym := st.Intern("table")
	st.Marshal(&buf, true)
	buf.BeginStruct(-1)
	buf.BeginField(tabsym)
	buf.WriteString(expr.ToString(is.tbl))
	buf.EndStruct()
	w, err := is.dst.Open()
	if err != nil {
		return err
	}
	_, err = w.Write(buf.Bytes())
	if err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

func (o *OutputIndex) exec(dst vm.QuerySink, parallel int, stat *ExecStats, rw TableRewrite) error {
	if o.Table == nil {
		return fmt.Errorf("OutputIndex: table not set")
	} else if o.Basename == "" {
		return fmt.Errorf("OutputIndex: basename not set")
	} else if o.Store == nil {
		return fmt.Errorf("OutputIndex: store not set")
	} else if o.Key == nil {
		return fmt.Errorf("OutputIndex: key not set")
	}
	tbl := &expr.Path{
		First: o.Table.First,
		Rest: &expr.Dot{
			Field: o.Table.Rest.(*expr.Dot).Field + "-" + uuid(),
		},
	}
	idx := &blockfmt.Index{
		Name: tbl.Rest.(*expr.Dot).Field,
		Algo: "zstd",
	}
	is := &indexSink{
		parent: o,
		tbl:    tbl,
		idx:    idx,
		dst:    dst,
	}
	err := o.From.exec(is, parallel, stat, rw)
	if err != nil {
		return err
	}
	return is.Close()
}

func (o *OutputIndex) setfield(d Decoder, name string, st *ion.Symtab, buf []byte) error {
	var err error
	switch name {
	case "table":
		e, _, err := expr.Decode(st, buf)
		if err != nil {
			return err
		}
		p, ok := e.(*expr.Path)
		if !ok {
			return fmt.Errorf("expected path, got %q", expr.ToString(e))
		}
		o.Table = p
	case "basename":
		o.Basename, _, err = ion.ReadString(buf)
	case "store":
		up, ok := d.(UploaderDecoder)
		if !ok {
			return fmt.Errorf("Decoder doesn't support UploaderDecoder: %T", d)
		}
		o.Store, err = up.DecodeUploader(st, buf)
	case "key":
		inner, _, err := ion.ReadBytesShared(buf)
		if err != nil {
			return err
		}
		if len(inner) != blockfmt.KeyLength {
			return fmt.Errorf("invalid key length: %d", len(inner))
		}
		o.Key = new(blockfmt.Key)
		copy(o.Key[:], inner)
	}
	return err
}

func (o *OutputIndex) encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.BeginStruct(-1)
	settype("outidx", dst, st)
	dst.BeginField(st.Intern("table"))
	o.Table.Encode(dst, st)
	dst.BeginField(st.Intern("basename"))
	dst.WriteString(o.Basename)
	dst.BeginField(st.Intern("store"))
	if err := o.Store.Encode(dst, st); err != nil {
		return err
	}
	dst.BeginField(st.Intern("key"))
	dst.WriteBlob(o.Key[:])
	dst.EndStruct()
	return nil
}

func (o *OutputIndex) String() string {
	return fmt.Sprintf("OUTPUT INDEX %s AT %s", expr.ToString(o.Table), o.Basename)
}
