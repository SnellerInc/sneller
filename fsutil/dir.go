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
	"io/fs"
	"os"
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
// entry in a directory. It is similar to
// fs.WalkDirFunc except that it lacks the name
// and err parameters.
type VisitDirFn func(d fs.DirEntry) error

// VisitDir calls fn for each entry in the
// directory specified by name, visiting each
// entry in lexicographical order.
//
// If seek is provided, all entries with names
// lexicographically preceeding seek are
// skipped.
//
// If pattern is provided, all entries with
// names not matching the pattern are skipped.
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
	if f, ok := f.(VisitDirFS); ok {
		return f.VisitDir(name, seek, pattern, fn)
	}
	list, err := fs.ReadDir(f, name)
	if err != nil {
		return err
	}
	for i := range list {
		visit, err := check(list[i].Name(), seek, pattern)
		if err != nil {
			return err
		}
		if !visit {
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

// WalkDir walks a file tree, calling fn for
// each file in the tree, including the root
// directory specified by name.
//
// If seek is provided, all paths in the file
// tree with names lexicographically preceeding
// seek are skipped.
//
// If pattern is provided, all entries with
// names not matching the pattern are skipped.
//
// The name parameter and the optional seek and
// pattern parameters (if provided) must be
// valid paths according to fs.ValidPath, or
// WalkDir will return *fs.PathError with Err
// set to fs.ErrInvalid.
//
// WalkDir is analogous to fs.WalkDir except
// that it uses VisitDir to walk the file tree,
// and accepts seek and pattern arguments which
// can be used to accelerate directory listing
// for file systems that implement VisitDirFS.
func WalkDir(f fs.FS, name, seek, pattern string, fn fs.WalkDirFunc) error {
	info, err := fs.Stat(f, name)
	if err != nil {
		err = fn(name, nil, err)
	} else {
		err = walkdir(f, name, seek, pattern, dirinfo{info}, fn)
	}
	if err == fs.SkipDir {
		return nil
	}
	return err
}

type dirinfo struct {
	fs.FileInfo
}

func (d dirinfo) Type() fs.FileMode          { return d.Mode().Type() }
func (d dirinfo) Info() (os.FileInfo, error) { return d.FileInfo, nil }

func walkdir(f fs.FS, name, seek, pattern string, d fs.DirEntry, walk fs.WalkDirFunc) error {
	// check if we should visit the entry by
	// checking against the whole seek/pattern
	visit, err := check(name, seek, pattern)
	if err != nil {
		return err
	}
	if visit {
		err := walk(name, d, nil)
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
		return errInvalid("walkdir", name)
	}
	seek0, seek1, ok := trim(seek, n)
	if !ok {
		return errInvalid("walkdir", seek)
	}
	pattern0, pattern1, ok := trim(pattern, n)
	if !ok {
		return errInvalid("walkdir", pattern)
	}
	if pathcmp(seek0, name) < 0 {
		// don't pass seek into VisitDir if we've
		// already passed the seek point
		seek1 = ""
	}
	descend, err := check(name, seek0, pattern0)
	if err != nil || !descend {
		return err
	}
	outer := func(d fs.DirEntry) error {
		full := path.Join(name, d.Name())
		return walkdir(f, full, seek, pattern, d, walk)
	}
	err = VisitDir(f, name, seek1, pattern1, outer)
	if err != nil {
		// report err to caller
		err = walk(name, d, err)
		if err != nil {
			if err == fs.SkipDir {
				err = nil
			}
			return err
		}
	}
	return nil
}

func errInvalid(op, path string) *fs.PathError {
	return &fs.PathError{Op: "walkdir", Path: path, Err: fs.ErrInvalid}
}

// check returns whether the path with the given
// name should be visited or descended into by
// WalkDir based on the seek and pattern
// parameters.
//
// The only way this can return an error is if
// pattern is malformed.
func check(name, seek, pattern string) (ok bool, err error) {
	ok = seek == "" || pathcmp(name, seek) >= 0
	if ok && pattern != "" {
		ok, err = path.Match(pattern, name)
	}
	return ok, err
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
