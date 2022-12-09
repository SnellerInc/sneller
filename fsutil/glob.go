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
)

// WalkGlobFn is the callback passed
// to WalkGlob that is called for each matching file.
//
// If WalkGlob encounters an error opening a file,
// then WalkGlobFn is called with a nil file and
// the error encountered opening the file;
// WalkGlob will continue if the error returned
// from the walk function is nil.
// Similarly, if the WalkGlobFn returns a non-nil error,
// then walking will stop.
type WalkGlobFn func(name string, file fs.File, err error) error

// Opener may be implemented by a DirEntry that
// can more efficiently open a file than calling
// fs.FS.Open on the file path.
type Opener interface {
	Open() (fs.File, error)
}

// WalkGlob opens all of the non-directory
// files in f that match pattern.
// The seek paramter determines the full path
// at which walking begins, and pattern indicates
// the glob pattern against which file paths are matched
// before being passed to the walk callback.
//
// WalkGlob uses WalkDir to walk the file tree.
// If it encounters an fs.DirEntry that implements
// Opener, Opener.Open will be used to open the file
// to pass to walk. Otherwise, fs.Open will be used.
func WalkGlob(f fs.FS, seek, pattern string, walk WalkGlobFn) error {
	pre := MetaPrefix(pattern)
	outer := func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return walk(p, nil, err)
		}
		var file fs.File
		if o, ok := d.(Opener); ok {
			file, err = o.Open()
		} else {
			file, err = f.Open(p)
		}
		return walk(p, file, err)
	}
	return WalkDir(f, pre, seek, pattern, outer)
}

// MetaPrefix finds the longest directory path for
// which we can begin searching for a glob pattern.
func MetaPrefix(pattern string) string {
	j := 0
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '*', '?', '\\', '[':
			if j == 0 {
				return "."
			}
			return pattern[:j]
		case '/':
			j = i
		}
	}
	return pattern
}

type NamedFile interface {
	fs.File
	Path() string
}

type namedFile struct {
	fs.File
	path string
}

func (n *namedFile) Path() string { return n.path }

// Named produces a NamedFile with name
// from an ordinary fs.File.
func Named(f fs.File, name string) NamedFile {
	if nf, ok := f.(NamedFile); ok {
		return nf
	}
	return &namedFile{f, name}
}

// OpenGlob performs a WalkGlob with the provided
// pattern and collects the results into a list
// of NamedFiles.
func OpenGlob(f fs.FS, pattern string) ([]NamedFile, error) {
	var out []NamedFile
	walk := func(name string, f fs.File, err error) error {
		if err != nil {
			return err
		}
		out = append(out, Named(f, name))
		return nil
	}
	err := WalkGlob(f, "", pattern, walk)
	return out, err
}
