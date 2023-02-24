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

package fsutil

import (
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strconv"
	"unicode/utf8"
)

// VisitDirFS can be implemented by a file
// system that provides an optimized
// implementation of VisitDir.
type VisitDirFS interface {
	fs.FS
	VisitDir(name, seek, pattern string, fn VisitDirFn) error
}

// VisitDirFn is called by VisitDir for each
// entry in a directory.
type VisitDirFn func(d DirEntry) error

// VisitDir calls fn for each entry in the
// directory specified by name, visiting each
// entry in lexicographical order.
//
// If seek is provided, only entries with names
// lexically succeeding seek are visited.
//
// If pattern is provided, only entries with
// names matching the pattern are visited.
//
// If fn returns fs.SkipDir, VisitDir returns
// immediately with a nil error.
//
// If f implements VisitDirFS, f.VisitDir is
// called directly, allowing the implementation
// to use the seek and pattern arguments to
// accelerate directory listing if possible.
// Otherwise, this simply calls fs.ReadDir and
// then calls fn for each matching entry.
func VisitDir(f fs.FS, name, seek, pattern string, fn VisitDirFn) error {
	if err := validpat(pattern); err != nil {
		return err
	}
	if f, ok := f.(VisitDirFS); ok {
		return f.VisitDir(name, seek, pattern, fn)
	}
	list, err := fs.ReadDir(f, name)
	if err != nil {
		return err
	}
	for i := range list {
		if n := list[i].Name(); n <= seek || !match(pattern, n) {
			continue
		}
		err = fn(list[i])
		if err != nil {
			if err == fs.SkipDir {
				break
			}
			return err
		}
	}
	return nil
}

// A DirEntry is an entry from a directory
// visited by VisitDir. This is analogous to
// fs.DirEntry without the Type() method.
type DirEntry interface {
	// Name is the file name of the file or
	// directory without additional path elements.
	Name() string
	// IsDir returns whether the entry is a
	// directory.
	IsDir() bool
	// Info returns the corresponding fs.FileInfo.
	Info() (fs.FileInfo, error)
}

// dirent is a DirEntry for which we only know
// the name, type (directory or file), and the
// file system that can be used to look it up.
type dirent struct {
	fs   fs.FS
	name string // full path name
	dir  bool
}

func (d *dirent) Name() string               { return path.Base(d.name) }
func (d *dirent) IsDir() bool                { return d.dir }
func (d *dirent) Info() (fs.FileInfo, error) { return fs.Stat(d.fs, d.name) }

// WalkDirFn is the callback called by WalkDir
// to visit each directory entry. It is
// analogous to fs.WalkDirFunc except that it
// takes a DirEntry from this package instead of
// fs.DirEntry.
type WalkDirFn func(path string, d DirEntry, err error) error

// WalkDir walks a file tree, calling fn for
// each file in the tree, including the root
// directory specified by name.
//
// If seek is provided, only entries with paths
// lexically succeeding the seek path are
// visited.
//
// If pattern is provided, only entries with
// paths matching the pattern are visited.
//
// The name parameter and the optional seek and
// pattern parameters (if provided) must be
// valid paths according to fs.ValidPath or
// WalkDir will return an error.
//
// WalkDir is analogous to fs.WalkDir except
// that it uses VisitDir to walk the file tree,
// and accepts seek and pattern arguments which
// can be used to accelerate directory listing
// for file systems that implement VisitDirFS.
func WalkDir(f fs.FS, name, seek, pattern string, fn WalkDirFn) error {
	if !fs.ValidPath(name) {
		return patherr("walkdir", name, fs.ErrInvalid)
	}
	err := validpat(pattern)
	if err != nil {
		return patherr("walkdir", name, err)
	}
	// if a seek path was provided, we can start
	// walking from seek without visiting
	// intermediate directories
	var d DirEntry
	if seek != "" {
		if !fs.ValidPath(seek) {
			return patherrf("walkdir", name, "bad seek %q", seek)
		}
		switch treecmp(name, seek) {
		case -1:
			// seek precedes tree: walk as usual
		case 0:
			// seek is within the tree: we can start
			// walking from the seek path
			seen, err := seekdir(f, name, seek, pattern, fn)
			if err != nil {
				return err
			}
			if seen {
				// we have confirmation name is a
				// directory, so no need to stat it
				d = &dirent{fs: f, name: name, dir: true}
			}
		case 1:
			// seek is beyond the tree: nothing to do
			return nil
		}
	}
	// everything we visit from here on out
	// comes after the seek point...
	if d == nil {
		d, err = stat(f, name)
		if err != nil {
			err = fn(name, nil, err)
			if err == fs.SkipDir {
				err = nil
			}
			return err
		}
	}
	err = walkdir(f, name, seek, pattern, d, fn)
	if err == fs.SkipDir {
		return nil
	}
	return err
}

// seekdir uses seek to determine whether we can
// start walking the tree from a known directory
// along the seek path.
//
// The return value indicates whether seekdir
// confirmed the existence of any entry along
// the seek path, which implies that name exists
// and is a directory.
//
// seek must lexically inside of the file tree
// rooted at name (see treecmp).
func seekdir(f fs.FS, name, seek, pattern string, fn WalkDirFn) (seen bool, err error) {
	for p := seek; p != name; p = path.Dir(p) {
		if treecmp(name, p) != 0 {
			// we somehow made our way outside of the
			// tree; this shouldn't happen, but avoid
			// a possible infinite loop...
			panic("fsutil.seekdir: seek is not within tree")
		}
		if seen {
			d := &dirent{fs: f, name: p, dir: true}
			err := walkdir(f, p, seek, pattern, d, fn)
			if err != nil && err != fs.SkipDir {
				return true, err
			}
			continue
		}
		d, err := stat(f, p)
		if errors.Is(err, fs.ErrNotExist) {
			continue // keep looking...
		} else if err != nil {
			err = fn(p, nil, err)
			if err != nil && err != fs.SkipDir {
				return false, err
			}
			continue
		}
		seen = true
		if d.IsDir() {
			err := walkdir(f, p, seek, pattern, d, fn)
			if err != nil && err != fs.SkipDir {
				return true, err
			}
		}
	}
	return seen, nil
}

func walkdir(f fs.FS, name, seek, pattern string, d DirEntry, fn WalkDirFn) error {
	// check if we should visit the entry by
	// checking against the whole seek/pattern
	visit := pathcmp(name, seek) > 0 && match(pattern, name)
	if visit {
		err := fn(name, d, nil)
		if err != nil || !d.IsDir() {
			if err == fs.SkipDir && d.IsDir() {
				err = nil
			}
			return err
		}
	}
	if !d.IsDir() {
		return nil
	}
	// check if we should descend into the
	// directory by checking against seek/pattern
	// trimmed to n segments
	n, ok := segments(name)
	if !ok {
		return patherr("walkdir", name, fs.ErrInvalid)
	}
	seek0, seek1, ok := trim(seek, n)
	if !ok {
		return patherrf("walkdir", name, "bad seek %q", seek)
	}
	pattern0, pattern1, ok := trim(pattern, n)
	if !ok {
		return patherrf("walkdir", name, "bad pattern %q", pattern)
	}
	if pattern0 != "" && pattern1 == "" {
		// no need to descend into the directory,
		// the pattern is not long enough to include
		// anything inside it...
		return nil
	}
	cmp := pathcmp(name, seek0)
	if cmp < 0 || !match(pattern0, name) {
		return nil
	}
	if cmp > 0 {
		// don't pass seek into VisitDir if we've
		// already passed the seek point
		seek1 = ""
	}
	outer := func(d DirEntry) error {
		full := path.Join(name, d.Name())
		return walkdir(f, full, seek, pattern, d, fn)
	}
	err := VisitDir(f, name, seek1, pattern1, outer)
	if err != nil {
		// report err to caller
		err = fn(name, d, err)
		if err != nil {
			if err == fs.SkipDir {
				err = nil
			}
			return err
		}
	}
	return nil
}

func patherrf(op, name, format string, args ...any) *fs.PathError {
	return patherr(op, name, fmt.Errorf(format, args...))
}

func patherr(op, name string, err error) *fs.PathError {
	return &fs.PathError{Op: op, Path: name, Err: err}
}

// pathcmp lexicographically compares the
// components of the paths a and b. The result
// is undefined if a or b are not valid paths.
//
// In many cases, this will be the same as
// strings.Compare, but will differ in cases
// where a path segment starts with a character
// that sorts before '/'.
func pathcmp(a, b string) int {
	// handle "" and "." first
	if a == "" || a == "." {
		if b == "" || b == "." {
			return 0
		}
		return -1
	}
	if b == "" || b == "." {
		return 1
	}
	for {
		ai, bi := 0, 0
		for ai < len(a) && a[ai] != '/' {
			ai++
		}
		for bi < len(b) && b[bi] != '/' {
			bi++
		}
		ae, be := a[:ai], b[:bi]
		if ae < be {
			return -1
		}
		if ae > be {
			return 1
		}
		if ai == len(a) {
			if bi == len(b) {
				return 0
			}
			return -1
		}
		if bi == len(b) {
			return 1
		}
		a, b = a[ai+1:], b[bi+1:]
	}
}

// treecmp lexically compares p to the entire
// path tree rooted at root. This will return 0
// if p lies somewhere within the tree
// (including the root), -1 if p lexically
// precedes the entire tree, or 1 if p lexically
// succeeds the entire tree.
//
// The result is undefined if root or p are not
// valid paths. In particular, neither root nor
// p may be "".
func treecmp(root, p string) int {
	if root == "." {
		return 0 // everything is in "."
	}
	if p == "." {
		return -1 // "." comes before any path
	}
	for {
		ri, pi := 0, 0
		for ri < len(root) && root[ri] != '/' {
			ri++
		}
		for pi < len(p) && p[pi] != '/' {
			pi++
		}
		re, pe := root[:ri], p[:pi]
		if re < pe {
			return 1 // p comes after
		}
		if re > pe {
			return -1 // p comes before
		}
		if ri == len(root) {
			return 0 // p is inside
		}
		if pi == len(p) {
			return -1 // tree is in p (p comes before)
		}
		root, p = root[ri+1:], p[pi+1:]
	}
}

// segments returns the number of path segments
// in the given path name, as well as a boolean
// value indicating whether name is valid.
//
// NOTE: "." is considered to have no segments
// so it is treated the same as ""
func segments(name string) (int, bool) {
	if name == "" || name == "." {
		return 0, true
	}
	if !utf8.ValidString(name) {
		return 0, false
	}
	n := 1
	for {
		i := 0
		for i < len(name) && name[i] != '/' {
			i++
		}
		elem := name[:i]
		if elem == "" || elem == "." || elem == ".." {
			return 0, false
		}
		if i == len(name) {
			return n, true
		}
		name = name[i+1:]
		n++
	}
}

// trim returns the first n segments of path p,
// as well as the next segment immediately
// following that, if any.
//
// If p is not a valid path (see fs.ValidPath)
// trim returns ("", "", false).
//
// If n < 0, trim panics.
func trim(p string, n int) (front, next string, ok bool) {
	if n < 0 {
		panic("fsutil: trim out of bounds: " + strconv.Itoa(n))
	}
	if p == "" || p == "." {
		// special case ("" or ".")
		if n == 0 {
			return "", p, true
		}
		return p, "", true
	}
	if !fs.ValidPath(p) {
		return "", "", false
	}
	seen := 0
	i := 0
	for {
		j := i
		for j < len(p) && p[j] != '/' {
			j++
		}
		elem := p[i:j]
		if elem == "" || elem == "." || elem == ".." {
			return "", "", false
		}
		if n == 0 {
			return "", elem, true
		}
		seen++
		if seen > n {
			return p[:i-1], elem, true
		}
		if j == len(p) {
			return p[:j], "", true
		}
		i = j + 1
	}
}

// validpat checks if a pattern is valid. If
// pattern is "", this returns nil.
func validpat(pattern string) error {
	if pattern == "" {
		return nil
	}
	_, err := path.Match(pattern, "")
	return err
}

// match should only be used if pattern has
// already been validated.
func match(pattern, name string) bool {
	if pattern == "" {
		return true
	}
	ok, _ := path.Match(pattern, name)
	return ok
}

func stat(f fs.FS, name string) (DirEntry, error) {
	info, err := fs.Stat(f, name)
	return fs.FileInfoToDirEntry(info), err
}
