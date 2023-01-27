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
	"sync"

	"github.com/SnellerInc/sneller/compr"
	"github.com/SnellerInc/sneller/ion"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

// fentry is the last-level leaf contents
// (typically a few thousand of these):
type fentry struct {
	// path is parent.raw[ppos:epos]
	// etag is parent.raw[epos:endpos]
	ppos, epos, endpos int
	// desc is the output file that
	// ended up containing this input,
	// or a negative number if the entry
	// has failed insertion
	desc int
}

// failed returns true if this insert
// is marked as failed
func (f *fentry) failed() bool {
	return f.desc < 0
}

// level is one tree node in a B+-tree; a level
// can either be an inner node (see isInner) or
// a leaf node
//
// leaf nodes contain a list of file entries (see fentry);
// inner nodes contain a list of further tree nodes
type level struct {
	// path and etag are the filepath and etag,
	// respectively, of the backing store for contents,
	// provided that this entry has been written out before
	//
	// if isDirty is set, then path may be nil or path
	// may point to a stale object
	path, etag []byte

	// last is the last path of all children of this node (recursively);
	// if this is a leaf node then it is contents[len(contents)-1].path
	last []byte

	// cached compressed contents; used to reduce memory footprint
	// compressed will never be non-nil when contents or levels is non-nil
	compressed []byte

	// contents is loaded lazily;
	// these are present if this is a leaf level
	contents []fentry
	// raw holds the etags and paths within contents;
	// it is not guaranteed to be sorted w.r.t contents
	// (this just makes it explicit where the aliased memory lives)
	// see also: level.compact()
	raw []byte

	// inner is loaded lazily;
	// these are present if this is an inner level
	levels []level

	// this is an inner node
	isInner bool
	// this node is dirty (path is stale or empty)
	isDirty bool
}

func (f *level) equalp(e *fentry, p string) bool {
	return bytes.Equal(f.entryPath(e), []byte(p))
}

func (f *level) entryPath(e *fentry) []byte {
	return f.raw[e.ppos:e.epos]
}

func (f *level) entryETag(e *fentry) []byte {
	return f.raw[e.epos:e.endpos]
}

func (f *level) set(e *fentry, path, etag string, id int) {
	e.ppos = len(f.raw)
	f.raw = append(f.raw, path...)
	e.epos = len(f.raw)
	f.raw = append(f.raw, etag...)
	e.endpos = len(f.raw)
	e.desc = id
}

// re-write f.raw for all of f.contents
// to only include the current entries (in sorted order)
func (f *level) compact() {
	var out []byte
	for i := range f.contents {
		c := &f.contents[i]
		p := f.entryPath(c)
		e := f.entryETag(c)
		c.ppos = len(out)
		out = append(out, p...)
		c.epos = len(out)
		out = append(out, e...)
		c.endpos = len(out)
	}
	f.raw = out
}

func (f *level) search(p string) int {
	if f.contents == nil {
		panic("level.search on non-leaf")
	}
	return sort.Search(len(f.contents), func(i int) bool {
		return bytes.Compare(f.entryPath(&f.contents[i]), []byte(p)) >= 0
	})
}

func (f *level) first() []byte {
	if len(f.contents) != 0 {
		return f.entryPath(&f.contents[0])
	}
	return nil
}

// return (appended, ok)
func (f *level) append(fs UploadFS, p, etag string, id int) (ret bool, err error) {
	if f.contents == nil && f.levels == nil {
		panic("level.append on un-loaded filetree")
	}
	if f.isInner {
		ret, err = f.appendInner(fs, p, etag, id)
	} else {
		ret, err = f.appendLeaf(p, etag, id)
	}
	// if we inserted anything, this entry is dirty:
	if ret {
		f.isDirty = true
	}
	return ret, err
}

func (f *level) appendLeaf(p, etag string, id int) (bool, error) {
	i := f.search(p)
	if i < len(f.contents) {
		ent := &f.contents[i]
		if f.equalp(ent, p) {
			ematch := string(f.entryETag(ent)) == etag
			if id < 0 && !ent.failed() && ematch {
				// allowed to turn OK -> failing
				ent.desc = id
				return true, nil
			} else if ent.failed() && !ematch {
				// allowed to modify ETag for failing entries
				f.set(ent, p, etag, id)
				return true, nil
			}
			var err error
			if !ematch {
				err = ErrETagChanged
			}
			return false, err
		}
		f.contents = append(f.contents, fentry{})
		copy(f.contents[i+1:], f.contents[i:])
		f.set(&f.contents[i], p, etag, id)
		return true, nil
	}
	// this is the largest item
	f.contents = append(f.contents, fentry{})
	c := &f.contents[len(f.contents)-1]
	f.set(c, p, etag, id)
	f.last = f.entryPath(c)
	return true, nil
}

type jentry struct {
	etag string
	id   int
}

type journal struct {
	entries map[string]jentry
}

func (j *journal) marshal(dst *ion.Buffer, st *ion.Symtab) {
	path := st.Intern("path")
	etag := st.Intern("etag")
	id := st.Intern("id")
	dst.BeginList(-1)
	for k, v := range j.entries {
		dst.BeginStruct(-1)
		dst.BeginField(path)
		dst.WriteString(k)
		dst.BeginField(etag)
		dst.WriteString(v.etag)
		dst.BeginField(id)
		dst.WriteInt(int64(v.id))
		dst.EndStruct()
	}
	dst.EndList()
}

func (j *journal) unmarshal(d ion.Datum) error {
	j.clear()
	return d.UnpackList(func(d ion.Datum) error {
		var path, etag string
		var id int64
		err := d.UnpackStruct(func(f ion.Field) error {
			var err error
			switch f.Label {
			case "path":
				path, err = f.String()
			case "etag":
				etag, err = f.String()
			case "id":
				id, err = f.Int()
			}
			return err
		})
		if err != nil {
			return err
		}
		if j.entries == nil {
			j.entries = make(map[string]jentry)
		}
		j.entries[path] = jentry{etag, int(id)}
		return nil
	})
}

func (j *journal) memsize() int {
	m := 0
	for k, v := range j.entries {
		m += len(k) + len(v.etag) + 8
	}
	return m
}

func (j *journal) append(path, etag string, id int) {
	if j.entries == nil {
		j.entries = make(map[string]jentry)
	}
	j.entries[path] = jentry{etag, id}
}

func (j *journal) clear() {
	if j.entries != nil {
		maps.Clear(j.entries)
	}
}

// FileTree is a B+-tree that holds a list of (path, etag, id)
// triples in sorted order by path.
type FileTree struct {
	// root is always an inner node with its levels
	// stored "inline" in the FileTree
	//
	// unlike other inner node in a B+ tree,
	// the root inner node may not always be at least half full
	//
	// oldroot is a shallow copy of root that is used
	// to write out the "old" state when we decide to
	// just write out a journal entry
	root    level
	oldroot ion.Datum

	// journal is a list of items that have been
	// committed to the tree but are not yet reflected
	// in the actual tree structure; these are stored in-line
	// in the root to reduce write latency
	journal journal

	// replayed is true if the journal contents
	// is reflected in the tree, or false otherwise
	replayed bool

	// Backing is the backing store for the tree.
	// Backing must be set to perform tree operations.
	Backing UploadFS
}

// this is adjusted in testing
//
// tree levels are only guaranteed to
// be half full, so worst-case capacity is:
// 2500 * 2500 = 6.25 million files in the second level
// 2500 cubed  = 15.625 billion files in the third level
var splitlevel = 5000

// lazily load a subtree
func (f *level) load(fs UploadFS) error {
	if f.isDirty || f.contents != nil || f.levels != nil {
		// already loaded
		return nil
	}
	if f.compressed != nil {
		if f.raw != nil {
			panic("level.compressed set but level.raw != nil ???")
		}
		// already loaded, but stored compressed
		raw := f.compressed
		f.compressed = nil
		return f.decode(raw)
	}
	p := string(f.path)
	file, err := fs.Open(p)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	etag, err := fs.ETag(p, info)
	if err != nil {
		return err
	}
	if etag != string(f.etag) {
		return fmt.Errorf("FileTree.load: etag mismatch for path %s (%s versus %s)", p, etag, string(f.etag))
	}
	// NOTE: we keep this buffer around
	// in the nodes themselves
	buf := make([]byte, info.Size())
	_, err = io.ReadFull(file, buf)
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	return f.decode(buf)
}

// XXX: unmarshalLeaf reuses the contents of d
// to avoid unnecessary allocations, which in
// turn causes d to be unusable after this
// method returns.
func (f *level) unmarshalLeaf(d ion.Datum) error {
	if !d.IsList() {
		return fmt.Errorf("unexpected ion type %s", d.Type())
	}
	f.isInner = false
	// subtle: we can re-use this buffer
	// because the data we stash is strictly
	// smaller than the original ion input
	f.raw = d.Raw()[:0]
	err := d.UnpackList(func(d ion.Datum) error {
		f.contents = append(f.contents, fentry{})
		fe := &f.contents[len(f.contents)-1]
		return d.UnpackStruct(func(sf ion.Field) error {
			var err error
			var id int64
			var contents []byte
			switch sf.Label {
			case "path":
				contents, err = sf.StringShared()
				if err == nil {
					fe.ppos = len(f.raw)
					f.raw = append(f.raw, contents...)
				}
			case "etag":
				contents, err = sf.StringShared()
				if err == nil {
					fe.epos = len(f.raw)
					f.raw = append(f.raw, contents...)
					fe.endpos = len(f.raw)
				}
			case "desc":
				id, err = sf.Int()
				if err == nil {
					fe.desc = int(id)
				}
			default:
				err = fmt.Errorf("unrecognized field name %q", sf.Label)
			}
			return err
		})
	})
	if err != nil {
		return err
	}
	if len(f.contents) == 0 {
		return fmt.Errorf("leaf with 0 entries at path %s", f.path)
	}
	return nil
}

func (f *level) compress(tmp *ion.Buffer, st *ion.Symtab) []byte {
	if f.isInner {
		return f.compressInner(tmp, st)
	}
	return f.compressLeaf(tmp, st)
}

func (f *level) compressInner(tmp *ion.Buffer, st *ion.Symtab) []byte {
	if !f.isInner {
		panic("compressInner called on leaf node")
	}
	if len(f.levels) == 0 {
		panic("compressInner on empty inner node")
	}
	st.Reset()
	tmp.Reset()
	f.encodeInner(tmp, st, true)
	c := compr.Compression("zstd")
	return c.Compress(tmp.Bytes(), nil)
}

func (f *level) compressLeaf(tmp *ion.Buffer, st *ion.Symtab) []byte {
	if f.isInner {
		panic("compressLeaf called on inner node")
	}
	if len(f.contents) == 0 {
		panic("compressLeaf on empty leaf")
	}
	st.Reset()
	tmp.Reset()
	pathsym := st.Intern("path")
	etagsym := st.Intern("etag")
	desc := st.Intern("desc")
	st.Marshal(tmp, true)
	tmp.BeginList(-1)
	for i := range f.contents {
		tmp.BeginStruct(-1)
		tmp.BeginField(pathsym)
		path := f.entryPath(&f.contents[i])
		tmp.WriteStringBytes(path)
		tmp.BeginField(etagsym)
		etag := f.entryETag(&f.contents[i])
		tmp.WriteStringBytes(etag)
		tmp.BeginField(desc)
		tmp.WriteInt(int64(f.contents[i].desc))
		tmp.EndStruct()
	}
	tmp.EndList()
	c := compr.Compression("zstd")
	ret := c.Compress(tmp.Bytes(), nil)
	return ret
}

func (f *FileTree) decode(d ion.Datum) error {
	readInner := func(d ion.Datum) error {
		err := f.root.unmarshalInner(d, true)
		if err != nil {
			return fmt.Errorf("FileTree.decode: unmarshalInner: %s", err)
		}
		f.oldroot = d
		return nil
	}
	switch t := d.Type(); t {
	case ion.StructType:
		return d.UnpackStruct(func(sf ion.Field) error {
			switch sf.Label {
			case "inner":
				return readInner(sf.Datum)
			case "journal":
				f.replayed = false
				return f.journal.unmarshal(sf.Datum)
			default:
				return fmt.Errorf("blockfmt.FileTree.decode: unrecognized field %q", sf.Label)
			}
		})
	case ion.ListType:
		f.journal.clear()
		f.replayed = true // empty journal; fully replayed
		return readInner(d)
	default:
		return fmt.Errorf("blockfmt.FileTree.decode: unexpected type %s", t)
	}
}

func (f *level) decode(in []byte) error {
	ret, err := compr.DecodeZstd(in, nil)
	if err != nil {
		return err
	}
	var st ion.Symtab
	ret, err = st.Unmarshal(ret)
	if err != nil {
		return err
	}
	d, _, err := ion.ReadDatum(&st, ret)
	if err != nil {
		return err
	}
	if f.isInner {
		return f.unmarshalInner(d, false)
	}
	return f.unmarshalLeaf(d)
}

// ErrETagChanged is returned by FileTree.Append
// when attempting to perform an insert with
// a file that has had its ETag change.
var ErrETagChanged = errors.New("FileTree: ETag changed")

// Prefetch takes a list of inputs and prefetches
// inner nodes or leaves that are likely to be
// associated with an insert operation on f.
// Currently, Prefetch only fetches "down" one
// level of the tree structure.
func (f *FileTree) Prefetch(input []Input) {
	if len(f.root.levels) == 0 {
		return // empty tree; nothing to do
	}
	var lst []string
	if f.replayed {
		lst = make([]string, len(input))
		for i := range input {
			lst[i] = input[i].Path
		}
	} else {
		// if the next Append operation will require
		// replaying the journal, then make sure we
		// prefetch leaves associated with journal
		// entries as well
		lst = make([]string, 0, len(input)+len(f.journal.entries))
		for i := range input {
			lst = append(lst, input[i].Path)
		}
		for path := range f.journal.entries {
			lst = append(lst, path)
		}
	}
	slices.Sort(lst)
	lst = slices.Compact(lst)
	f.root.prefetchInner(f.Backing, lst)
}

func (f *level) prefetchInner(src UploadFS, lst []string) {
	var wg sync.WaitGroup
	// we sub-divide the sorted input list
	// into groups that correspond to sub-trees
	// and then dispatch those recursively
	type group struct {
		pos    int // level position
		lstpos int // input name list position
	}

	var groups []group
	for j, name := range lst {
		pos := sort.Search(len(f.levels), func(i int) bool {
			// find the lowest toplevel entry w/
			// path <= largest path
			return bytes.Compare([]byte(name), f.levels[i].last) <= 0
		})
		if pos == len(f.levels) {
			pos--
		}
		// list should be sorted, so see if we
		// sorted into the previous group
		if len(groups) > 0 && groups[len(groups)-1].pos == pos {
			continue
		}
		// sanity check: input list should be sorted
		if len(groups) > 0 && pos < groups[len(groups)-1].pos {
			panic("pos out-of-order")
		}
		// collect groups for all non-populated entries
		if pos < len(f.levels) {
			groups = append(groups, group{
				pos:    pos,
				lstpos: j,
			})
		}
	}
	for i, grp := range groups {
		start := grp.lstpos
		end := len(lst)
		if i < len(groups)-1 {
			end = groups[i+1].lstpos
		}
		names := lst[start:end]
		ent := &f.levels[grp.pos]
		if ent.contents != nil || ent.levels != nil {
			continue // level already present
		}
		wg.Add(1)
		go func(lst []string) {
			defer wg.Done()
			ent.prefetch(src, lst)
		}(names)
	}
	wg.Wait()
}

func (f *level) prefetch(src UploadFS, paths []string) {
	if f.load(src) != nil {
		return
	}
	if f.isInner {
		f.prefetchInner(src, paths)
	}
}

// ensure journal contents are reflected in the tree
func (f *FileTree) replay() error {
	if f.replayed {
		return nil
	}
	for k, v := range f.journal.entries {
		ret, err := f.root.appendInner(f.Backing, k, v.etag, v.id)
		if err != nil {
			return err
		}
		// each of these should be a new entry
		if !ret {
			panic("FileTree.replay: journal and tree out-of-sync")
		}
	}
	if len(f.root.levels) >= splitlevel {
		f.root.split()
	}
	f.replayed = true
	return nil
}

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
	// the root is always constructed as a non-leaf entry;
	// make sure as soon as anything is inserted that the
	// root is marked correctly
	f.root.isInner = true
	err := f.replay()
	if err != nil {
		return false, err
	}
	// perform a real tree insert:
	ret, err := f.root.appendInner(f.Backing, path, etag, id)
	if err != nil || !ret {
		return ret, err
	}
	if !f.root.isDirty {
		panic("appendInner succeeded w/o setting isDirty")
	}
	// replicate the tree state in the journal:
	f.journal.append(path, etag, id)
	// once we accumulate enough data in the root,
	// split the root itself into more inner nodes
	if len(f.root.levels) >= splitlevel {
		f.root.split()
	}
	return ret, err
}

// split a level into two inner levels
func (f *level) split() {
	if !f.isInner {
		panic("level.split() on non-inner level")
	}
	lower, upper := safeSplit(f.levels)
	f.isDirty = true
	f.levels = []level{
		level{
			levels:  lower,
			last:    lower[len(lower)-1].last,
			isDirty: true,
			isInner: true,
		},
		level{
			levels:  upper,
			last:    upper[len(upper)-1].last,
			isDirty: true,
			isInner: true,
		},
	}
}

func (f *level) checkSplit(parent *level, pos int) {
	if f.isInner {
		if len(f.levels) >= splitlevel {
			f.splitInner(parent, pos)
		}
	} else {
		if len(f.contents) >= splitlevel {
			f.splitLeaf(parent, pos)
		}
	}
}

// split x into two pieces, taking care
// to limit the capacity of the lower half
// so that appends do not clobber the upper half
func safeSplit[T any](x []T) ([]T, []T) {
	l := len(x)
	return slices.Clip(x[:l/2]), x[l/2:]
}

func (f *level) splitInner(parent *level, pos int) {
	if !parent.isInner {
		panic("level.splitInner: !parent.isInner")
	}
	if f != &parent.levels[pos] {
		panic("level.splitInner: invalid argument")
	}

	// cut f in half:
	lower, upper := safeSplit(f.levels)
	f.levels = lower
	f.last = lower[len(lower)-1].last
	f.isDirty = true

	// push a new level to parent
	parent.insertAt(pos+1, level{
		last:    upper[len(upper)-1].last,
		levels:  upper,
		isInner: true,
		isDirty: true,
	})
}

// insertAt inserts a new entry at a particular
// position in an inner level
func (f *level) insertAt(pos int, lvl level) {
	if !f.isInner {
		panic("level.insertAt called on leaf level")
	}
	if lvl.levels == nil && lvl.contents == nil {
		panic("level.insertAt empty level")
	}
	f.isDirty = true

	f.levels = append(f.levels, lvl)
	if pos == len(f.levels)-1 {
		f.last = lvl.last
		return
	}
	// copy everything forwards and overwrite
	copy(f.levels[pos+1:], f.levels[pos:])
	f.levels[pos] = lvl
}

func (f *level) splitLeaf(parent *level, pos int) {
	if !parent.isInner {
		panic("level.splitLeaf: !parent.isInner")
	}
	if f != &parent.levels[pos] {
		panic("level.splitLeaf: bad argument")
	}
	raw := f.raw

	// update existing entry to use just
	// half of its current contents
	lower, upper := safeSplit(f.contents)
	f.contents = lower
	// make a smaller copy of f.raw
	f.compact()
	f.last = f.entryPath(&lower[len(lower)-1])
	f.isDirty = true

	newlvl := level{
		contents: upper,
		raw:      raw,
		isDirty:  true,
	}
	// make a smaller copy of f.raw
	newlvl.compact()
	newlvl.last = newlvl.entryPath(&upper[len(upper)-1])
	// push a new entry into the parent
	// just past the position of the lower half
	parent.insertAt(pos+1, newlvl)
}

func (f *level) appendInner(fs UploadFS, path, etag string, id int) (bool, error) {
	i := sort.Search(len(f.levels), func(i int) bool {
		// find the lowest toplevel entry w/
		// path <= largest path
		return bytes.Compare([]byte(path), f.levels[i].last) <= 0
	})
	if i == len(f.levels) {
		if len(f.levels) == 0 {
			// create new leaf
			l := level{isDirty: true}
			l.contents = append(l.contents, fentry{})
			c := &l.contents[len(l.contents)-1]
			l.set(c, path, etag, id)
			l.last = l.entryPath(c)

			// update parent
			f.levels = append(f.levels, l)
			f.last = l.last
			f.isDirty = true
			return true, nil
		}
		// just append to the last entry
		i--
	}
	ent := &f.levels[i]
	err := ent.load(fs)
	if err != nil {
		return false, fmt.Errorf("load entry (inner: %v) @ %d: %s", ent.isInner, i, err)
	}
	ret, err := ent.append(fs, path, etag, id)
	if err != nil || !ret {
		return ret, err
	}
	f.isDirty = true
	if i == len(f.levels)-1 {
		// ensure last is up-to-date if we
		// dirtied the last inner level
		f.last = f.levels[i].last
	}
	ent.checkSplit(f, i)
	return true, nil
}

type syncfn func(oldpath string, mem []byte) (path, etag string, err error)

func (f *FileTree) sync(fn syncfn) error {
	const (
		// with fewer than 50 items in the journal,
		// don't bother writing out a new tree
		minJournaled = 50
		// ... but always write out the tree if we
		// are using more than 10MB of memory for leaf nodes
		// (we can't buffer forever)
		maxLeafResident = 10 * 1024 * 1024
	)
	resident := f.root.leafSizes() + f.journal.memsize()
	if !f.root.isDirty ||
		(len(f.journal.entries) < minJournaled && resident < maxLeafResident) {
		return nil
	}
	// launch all of the uploads asynchronously
	// and then wait for all of them to complete;
	// the sync process also evicts all non-dirty leaf nodes
	// and re-compresses active leaf nodes in order to keep
	// our memory use under control
	err := f.root.syncInner(fn)
	if err != nil {
		return err
	}
	// if we write out a new tree, then
	// this journal is committed and we can restart
	f.journal.clear()
	{
		// update oldroot; we maintain the invariant
		// that oldroot is the root associated with the
		// tree state that has been synchronized
		var st ion.Symtab
		var buf ion.Buffer
		f.root.encodeInner(&buf, &st, false)
		f.oldroot, _, err = ion.ReadDatum(&st, buf.Bytes())
		if err != nil {
			panic(err) // should not be possible
		}
	}
	return nil
}

func (f *level) syncInner(fn syncfn) error {
	var wg sync.WaitGroup
	errc := make(chan error, 1)

	dosync := func(t *level) {
		defer wg.Done()
		if t.isInner {
			// we cannot compress this node
			// until all its children have
			// been assigned pathnames + etags
			err := t.syncInner(fn)
			if err != nil {
				errc <- err
				return
			}
		}
		var st ion.Symtab
		var buf ion.Buffer
		contents := t.compress(&buf, &st)
		path, etag, err := fn(string(t.path), contents)
		if err != nil {
			errc <- err
			return
		}
		t.compressed = contents
		// we can set path and ditch oldpath
		t.path = []byte(path)
		t.etag = []byte(etag)
		// keep the compressed representation in memory,
		// not the decompressed raw data (saves ~10x)
		t.levels = nil
		t.contents = nil
		t.raw = nil
		t.isDirty = false
		errc <- nil
	}
	for i := range f.levels {
		t := &f.levels[i]
		if !t.isDirty {
			// evict clean entries across syncs
			t.dropLeaves()
			continue
		}
		wg.Add(1)
		go dosync(t)
	}
	go func() {
		wg.Wait()
		close(errc)
	}()
	// take the first error:
	var err error
	for e := range errc {
		if e != nil && err == nil {
			err = e
		}
	}
	f.isDirty = err != nil
	return err
}

func (f *FileTree) encode(dst *ion.Buffer, st *ion.Symtab) {
	f.root.isInner = true
	if len(f.journal.entries) > 0 {
		dst.BeginStruct(-1)
		dst.BeginField(st.Intern("inner"))
		if f.oldroot.IsEmpty() {
			// only journal entries; just encode [] as the tree
			dst.BeginList(-1)
			dst.EndList()
		} else {
			f.oldroot.Encode(dst, st)
		}
		dst.BeginField(st.Intern("journal"))
		f.journal.marshal(dst, st)
		dst.EndStruct()
		return
	}
	f.root.encodeInner(dst, st, false)
}

func (f *level) encodeInner(dst *ion.Buffer, st *ion.Symtab, inline bool) {
	lastsym := st.Intern("last")
	pathsym := st.Intern("path")
	etagsym := st.Intern("etag")
	innersym := st.Intern("is_inner")
	if inline {
		st.Marshal(dst, true)
		if len(f.levels) == 0 {
			panic("inline inner node with zero entries")
		}
	}
	dst.BeginList(-1)
	for i := range f.levels {
		if f.levels[i].isDirty {
			panic("level.encodeInner with dirty elements")
		}
		dst.BeginStruct(-1)
		dst.BeginField(lastsym)
		dst.WriteStringBytes(f.levels[i].last)
		dst.BeginField(pathsym)
		dst.WriteStringBytes(f.levels[i].path)
		dst.BeginField(etagsym)
		dst.WriteStringBytes(f.levels[i].etag)
		if f.levels[i].isInner {
			dst.BeginField(innersym)
			dst.WriteBool(true)
		}
		dst.EndStruct()
	}
	dst.EndList()
}

// unmarshalInner unmarshals an inner node's contents
//
// NOTE: f.levels[i].{path,etag,last} will alias d!
func (f *level) unmarshalInner(d ion.Datum, root bool) error {
	if f.contents != nil {
		panic("level.unmarshalInner on leaf")
	}
	f.isInner = true
	err := d.UnpackList(func(d ion.Datum) error {
		f.levels = append(f.levels, level{})
		fe := &f.levels[len(f.levels)-1]
		return d.UnpackStruct(func(sf ion.Field) error {
			var dst *[]byte
			var err error
			switch sf.Label {
			case "last":
				dst = &fe.last
			case "path":
				dst = &fe.path
			case "etag":
				dst = &fe.etag
			case "is_inner":
				fe.isInner, err = sf.Bool()
				return err
			}
			if dst == nil {
				return fmt.Errorf("unrecognized filetree field %q", sf.Label)
			}
			*dst, err = sf.StringShared()
			return err
		})
	})
	if !root && len(f.levels) == 0 {
		return fmt.Errorf("inner node with 0 entries?")
	}
	if err == nil && len(f.levels) > 0 {
		f.last = f.levels[len(f.levels)-1].last
	}
	return err
}

// Reset resets the contents of the tree
func (f *FileTree) Reset() {
	// reset all private fields
	// except f.root.isInner
	*f = FileTree{
		Backing: f.Backing,
	}
	f.root.isInner = true
}

// EachFile iterates the backing of f and
// calls fn for each file that is pointed
// to by the FileTree (i.e. files that contain
// either inner nodes or leaf nodes that still
// constitute part of the tree state).
func (f *FileTree) EachFile(fn func(filename string)) error {
	return f.root.eachFile(f.Backing, fn)
}

func (f *level) eachFile(fs UploadFS, fn func(filename string)) error {
	for i := range f.levels {
		// depending on the state of the tree,
		// the pointed-to files may be either in
		// oldpath or in path
		if f.levels[i].path != nil {
			fn(string(f.levels[i].path))
		}
		if f.levels[i].isInner {
			err := f.levels[i].load(fs)
			if err != nil {
				return err
			}
			err = f.levels[i].eachFile(fs, fn)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// drop drops the lazily-loaded contents
// of a tree node, but only if it is a leaf
// and it is not dirty
func (f *level) drop() {
	// since f.last usually aliases f.raw,
	// we can only cause f.raw to be GC'd if
	// we produce a fresh copy:
	f.last = slices.Clone(f.last)
	if !f.isDirty && !f.isInner {
		f.contents = nil
		f.raw = nil
		f.compressed = nil
	}
}

// drop all leaf nodes of the tree
func (f *level) dropLeaves() {
	if f.isInner {
		for i := range f.levels {
			f.levels[i].dropLeaves()
		}
		return
	}
	f.drop()
}

// leafSizes computes the total amount
// of memory devoted to leaf nodes in the tree
func (f *level) leafSizes() int {
	if f.isInner {
		sz := 0
		for i := range f.levels {
			sz += f.levels[i].leafSizes()
		}
		return sz
	}
	if f.compressed != nil {
		return len(f.compressed)
	}
	return len(f.raw)
}

// Walk performs an in-order walk of the filetree
// starting at the first item greater than or
// equal to start. The walk function is called
// on each item in turn until it returns false
// or until an I/O error is encountered.
func (f *FileTree) Walk(start string, walk func(name, etag string, id int) bool) error {
	err := f.replay()
	if err != nil {
		return err
	}
	err, _ = f.root.walkInner(f.Backing, start, walk)
	return err
}

type walkFunc func(name, etag string, id int) bool

func (f *level) walkLeaf(start string, walk walkFunc) (error, bool) {
	for j := f.search(start); j < len(f.contents); j++ {
		name := string(f.entryPath(&f.contents[j]))
		etag := string(f.entryETag(&f.contents[j]))
		id := f.contents[j].desc
		if !walk(name, etag, id) {
			return nil, false
		}
	}
	return nil, true
}

func (f *level) walk(fs UploadFS, start string, walk walkFunc) (error, bool) {
	if f.isInner {
		return f.walkInner(fs, start, walk)
	}
	return f.walkLeaf(start, walk)
}

func (f *level) walkInner(fs UploadFS, start string, walk walkFunc) (error, bool) {
	i := sort.Search(len(f.levels), func(i int) bool {
		// find the lowest toplevel entry w/
		// path <= largest path
		return bytes.Compare([]byte(start), f.levels[i].last) <= 0
	})
	if i >= len(f.levels) {
		// start > largest entry
		return nil, false
	}
	var cont bool
	for ; i < len(f.levels); i++ {
		err := f.levels[i].load(fs)
		if err != nil {
			return err, false
		}
		err, cont = f.levels[i].walk(fs, start, walk)
		f.levels[i].drop()
		if err != nil || !cont {
			return err, cont
		}
	}
	return nil, true
}
