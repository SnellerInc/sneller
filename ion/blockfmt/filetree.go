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

package blockfmt

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/SnellerInc/sneller/ion"
	"github.com/klauspost/compress/zstd"
)

// last-level contents
type fentry struct {
	// path and etag are the raw
	// filepath and ETag of the input
	path, etag []byte
	// desc is the output file that
	// ended up containing this input,
	// or a negative number if the entry
	// has failed insertion
	desc int
}

func (f *fentry) equalp(p string) bool {
	return bytes.Equal(f.path, []byte(p))
}

// failed returns true if this insert
// is marked as failed
func (f *fentry) failed() bool {
	return f.desc < 0
}

// 2nd-level contents
type filetree struct {
	// path and etag are the filepath and etag,
	// respectively, of the backing store for contents,
	// provided that this entry has been written out before
	path, etag []byte
	// last is equal to contents[len(contents)-1.path],
	// but it is stored verbatim rather than out-of-band
	last []byte

	// contents is loaded lazily
	contents []fentry
}

func (f *filetree) size() int {
	return len(f.contents)
}

func (f *filetree) search(p string) int {
	if f.contents == nil {
		panic("filetree.search on un-loaded filtree")
	}
	return sort.Search(len(f.contents), func(i int) bool {
		return bytes.Compare(f.contents[i].path, []byte(p)) >= 0
	})
}

func (f *filetree) first() []byte {
	if len(f.contents) != 0 {
		return f.contents[0].path
	}
	return nil
}

// return (appended, ok)
func (f *filetree) append(p, etag string, id int) (bool, bool) {
	if f.contents == nil {
		panic("filetree.insert on un-loaded filetree")
	}
	i := f.search(p)
	if i < len(f.contents) {
		ent := &f.contents[i]
		if ent.equalp(p) {
			if id < 0 && !ent.failed() && string(ent.etag) == etag {
				// allowed to turn OK -> failing
				ent.desc = id
				return true, true
			} else if ent.failed() && string(ent.etag) != etag {
				// allowed to modify ETag for failing entries
				ent.desc = id
				ent.etag = []byte(etag)
				return true, true
			}
			return false, string(ent.etag) == etag
		}
		f.contents = append(f.contents, fentry{})
		copy(f.contents[i+1:], f.contents[i:])
		f.contents[i].path = []byte(p)
		f.contents[i].etag = []byte(etag)
		f.contents[i].desc = id
		return true, true
	}
	// this is the largest item
	key := []byte(p)
	f.contents = append(f.contents, fentry{
		path: key,
		etag: []byte(etag),
		desc: id,
	})
	f.last = key
	return true, true
}

type FileTree struct {
	dirty    []bool
	toplevel []filetree

	Backing UploadFS
}

const splitlevel = 5000

// lazily load a subtree
func (f *FileTree) load(src *filetree) error {
	if len(src.contents) > 0 {
		return nil
	}
	p := string(src.path)
	file, err := f.Backing.Open(p)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	etag, err := f.Backing.ETag(p, info)
	if err != nil {
		return err
	}
	if etag != string(src.etag) {
		return fmt.Errorf("FileTree.load: etag mismatch for path %s (%s versus %s)", p, etag, string(src.etag))
	}
	// TODO: src aliases buf, so if
	// we know when the FileTree is dropped,
	// we could recycle this buffer
	buf := make([]byte, info.Size())
	_, err = io.ReadFull(file, buf)
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	return src.decode(buf)
}

func (f *filetree) unmarshal(in []byte) error {
	var st ion.Symtab
	var err error

	in, err = st.Unmarshal(in)
	if err != nil {
		return err
	}
	if ion.TypeOf(in) != ion.ListType {
		return fmt.Errorf("unexpected ion type %s", ion.TypeOf(in))
	}
	return unpackList(in, func(item []byte) error {
		f.contents = append(f.contents, fentry{})
		fe := &f.contents[len(f.contents)-1]
		return unpackStruct(&st, item, func(name string, field []byte) error {
			var err error
			var id int64
			switch name {
			case "path":
				fe.path, _, err = ion.ReadStringShared(field)
			case "etag":
				fe.etag, _, err = ion.ReadStringShared(field)
			case "desc":
				id, _, err = ion.ReadInt(field)
				if err == nil {
					fe.desc = int(id)
				}
			default:
				err = fmt.Errorf("unrecognized field name %q", name)
			}
			return err
		})
	})
}

func (f *filetree) marshal(tmp *ion.Buffer, st *ion.Symtab) []byte {
	st.Reset()
	tmp.Reset()
	pathsym := st.Intern("path")
	etag := st.Intern("etag")
	desc := st.Intern("desc")
	st.Marshal(tmp, true)
	tmp.BeginList(-1)
	for i := range f.contents {
		tmp.BeginStruct(-1)
		tmp.BeginField(pathsym)
		tmp.BeginString(len(f.contents[i].path))
		tmp.UnsafeAppend(f.contents[i].path)
		tmp.BeginField(etag)
		tmp.BeginString(len(f.contents[i].etag))
		tmp.UnsafeAppend(f.contents[i].etag)
		tmp.BeginField(desc)
		tmp.WriteInt(int64(f.contents[i].desc))
		tmp.EndStruct()
	}
	tmp.EndList()
	enc, _ := zstd.NewWriter(nil)
	ret := enc.EncodeAll(tmp.Bytes(), nil)
	enc.Close()
	return ret
}

func (f *filetree) decode(in []byte) error {
	ret, err := theDecoder.DecodeAll(in, nil)
	if err != nil {
		return err
	}
	return f.unmarshal(ret)
}

// ErrETagChanged is returned by FileTree.Append
// when attempting to perform an insert with
// a file that has had its ETag change.
var ErrETagChanged = errors.New("FileTree: ETag changed")

// Append assigns an ID to a path and etag.
// Append returns (true, nil) if the (path, etag)
// tuple is inserted, or (false, nil) if the (path, etag)
// tuple has already been inserted.
// A tuple may be inserted under the following conditions:
//
//  1. The path has never been appended before.
//  2. The (path, etag) pair has been inserted with an id >= 0,
//     and it is being marked as failed by being re-inserted
//     with an id < 0.
//  3. The same path but a different etag has been marked
//     as failed (with id < 0), and Append is overwriting
//     the previous entry with a new etag and id >= 0.
//
// Otherwise, if there exists a (path, etag) tuple with a matching
// path but non-matching etag, then (false, ErrETagChanged)
// is returned.
func (f *FileTree) Append(path, etag string, id int) (bool, error) {
	i := sort.Search(len(f.toplevel), func(i int) bool {
		// find the lowest toplevel entry w/
		// path <= largest path
		return bytes.Compare([]byte(path), f.toplevel[i].last) <= 0
	})
	if i == len(f.toplevel) {
		key := []byte(path)
		// inserting above the max key;
		if len(f.toplevel) > 0 {
			tail := &f.toplevel[i-1]
			err := f.load(tail)
			if err != nil {
				return false, err
			}
			if tail.size() < splitlevel {
				f.dirty[i-1] = true
				ret, ok := tail.append(path, etag, id)
				if !ok || !ret {
					panic("tail insert failed...?")
				}
				return true, nil
			}
		}
		// appending a new intermediate level
		f.dirty = append(f.dirty, true)
		f.toplevel = append(f.toplevel, filetree{
			last: key,
			contents: []fentry{{
				path: key,
				etag: []byte(etag),
				desc: id,
			}},
		})
		return true, nil
	}

	// intermediate insert
	ent := &f.toplevel[i]
	err := f.load(ent)
	if err != nil {
		return false, err
	}
	ret, ok := ent.append(path, etag, id)
	if !ok {
		return false, ErrETagChanged
	}
	f.dirty[i] = f.dirty[i] || ret
	if ent.size() <= splitlevel {
		return ret, nil
	}
	f.dirty[i] = true

	// split the entry in half:
	split := len(ent.contents) / 2
	before := ent.contents[:split:split] // ensure appends to not mess w/ after
	after := ent.contents[split:]
	ent.contents = before
	ent.last = before[len(before)-1].path

	// ... otherwise,
	// insertion-sort a new filetree
	i++
	f.dirty = append(f.dirty, true)
	copy(f.dirty[i+1:], f.dirty[i:])
	f.dirty[i] = true
	f.toplevel = append(f.toplevel, filetree{})
	copy(f.toplevel[i+1:], f.toplevel[i:])
	f.toplevel[i].path = nil
	f.toplevel[i].etag = nil
	f.toplevel[i].last = after[len(after)-1].path
	f.toplevel[i].contents = after
	return true, nil
}

type syncfn func(oldpath string, mem []byte) (path, etag string, err error)

func (f *FileTree) sync(fn syncfn) error {
	var st ion.Symtab
	var buf ion.Buffer
	for i := range f.toplevel {
		if !f.dirty[i] {
			continue
		}
		buf := f.toplevel[i].marshal(&buf, &st)
		path, etag, err := fn(string(f.toplevel[i].path), buf)
		if err != nil {
			return err
		}
		f.toplevel[i].path = []byte(path)
		f.toplevel[i].etag = []byte(etag)
		f.dirty[i] = false
	}
	return nil
}

func (f *FileTree) encode(dst *ion.Buffer, st *ion.Symtab) {
	lastsym := st.Intern("last")
	pathsym := st.Intern("path")
	etagsym := st.Intern("etag")
	dst.BeginList(-1)
	for i := range f.toplevel {
		if f.dirty[i] {
			panic("FileTree.encode: dirty entries")
		}
		dst.BeginStruct(-1)
		dst.BeginField(lastsym)
		dst.BeginString(len(f.toplevel[i].last))
		dst.UnsafeAppend(f.toplevel[i].last)
		dst.BeginField(pathsym)
		dst.BeginString(len(f.toplevel[i].path))
		dst.UnsafeAppend(f.toplevel[i].path)
		dst.BeginField(etagsym)
		dst.BeginString(len(f.toplevel[i].etag))
		dst.UnsafeAppend(f.toplevel[i].etag)
		dst.EndStruct()
	}
	dst.EndList()
}

func (f *FileTree) decode(st *ion.Symtab, in []byte) error {
	return unpackList(in, func(field []byte) error {
		f.toplevel = append(f.toplevel, filetree{})
		f.dirty = append(f.dirty, false)
		fe := &f.toplevel[len(f.toplevel)-1]
		return unpackStruct(st, field, func(name string, field []byte) error {
			var dst *[]byte
			var err error
			switch name {
			case "first":
				return nil // ignored for backwards-compatibility; remove me!
			case "last":
				dst = &fe.last
			case "path":
				dst = &fe.path
			case "etag":
				dst = &fe.etag
			}
			if dst == nil {
				return fmt.Errorf("unrecognized filetree field %q", name)
			}
			*dst, _, err = ion.ReadStringShared(field)
			return err
		})
	})
}

// Reset resets the contents of the tree
func (f *FileTree) Reset() {
	f.toplevel = nil
	f.dirty = nil
}

// Contains returns whether or not f contains
// the path p.
func (f *FileTree) Contains(p string) (bool, error) {
	i := sort.Search(len(f.toplevel), func(i int) bool {
		// find the lowest toplevel entry w/
		// path <= largest path
		return bytes.Compare([]byte(p), f.toplevel[i].last) <= 0
	})
	if i >= len(f.toplevel) {
		return false, nil
	}
	ent := &f.toplevel[i]
	err := f.load(ent)
	if err != nil {
		return false, err
	}
	j := ent.search(p)
	return j < len(ent.contents) && ent.contents[j].equalp(p), nil
}

// EachFile calls fn once for each
// file that currently holds part of
// the contents of f.
func (f *FileTree) EachFile(fn func(filename string)) {
	for i := range f.toplevel {
		if len(f.toplevel[i].path) != 0 {
			fn(string(f.toplevel[i].path))
		}
	}
}

func (f *FileTree) drop(i int) {
	if !f.dirty[i] {
		f.toplevel[i].contents = nil
	}
}

// Walk performs an in-order walk of the filetree
// starting at the first item greater than or
// equal to start. The walk function is called
// on each item in turn until it returns false
// or until an I/O error is encountered.
func (f *FileTree) Walk(start string, walk func(name, etag string, id int) bool) error {
	i := sort.Search(len(f.toplevel), func(i int) bool {
		// find the lowest toplevel entry w/
		// path <= largest path
		return bytes.Compare([]byte(start), f.toplevel[i].last) <= 0
	})
	if i >= len(f.toplevel) {
		// start > largest entry
		return nil
	}
	// first entry: fast-forward to cursor
	err := f.load(&f.toplevel[i])
	if err != nil {
		return err
	}
	for j := f.toplevel[i].search(start); j < len(f.toplevel[i].contents); j++ {
		name := string(f.toplevel[i].contents[j].path)
		etag := string(f.toplevel[i].contents[j].etag)
		id := f.toplevel[i].contents[j].desc
		if !walk(name, etag, id) {
			return nil
		}
	}
	// limit total memory usage
	// by dropping each subtree
	// when we are done scanning it
	f.drop(i)
	i++
	for ; i < len(f.toplevel); i++ {
		err := f.load(&f.toplevel[i])
		if err != nil {
			return err
		}
		for j := range f.toplevel[i].contents {
			name := string(f.toplevel[i].contents[j].path)
			etag := string(f.toplevel[i].contents[j].etag)
			id := f.toplevel[i].contents[j].desc
			if !walk(name, etag, id) {
				return nil
			}
		}
		f.drop(i)
	}
	return nil
}
