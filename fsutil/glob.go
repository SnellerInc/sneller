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

// Package fsutil defines functions and interfaces for
// working with file systems.
package fsutil

import (
	"fmt"
	"io/fs"
	"path"
	"strings"
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

// WalkGlobFS is an interface implemented
// by filesystems that have an optimized
// OpenGlob implementation.
type WalkGlobFS interface {
	fs.FS
	WalkGlob(seek, pattern string, walk WalkGlobFn) error
}

// WalkGlob opens all of the non-directory
// files in f that match pattern.
// The seek paramter determines the full path
// at which walking begins, and pattern indicates
// the glob pattern against which file paths are matched
// before being passed to the walk callback.
//
// The seek string must match a prefix of pattern;
// WalkGlob will return an error if this invariant
// is not preserved. If seek is not the empty string,
// then seek must also indicate a position that is lexicographically
// at or above the "smallest" string matched by pattern.
//
// If f implements WalkGlobFS, then f.WalkGlob is called directly.
// Otherwise, WalkGlob calls fs.Glob and opens the files sequentially.
// (See also fs.Glob, fs.GlobFS.)
func WalkGlob(f fs.FS, seek, pattern string, walk WalkGlobFn) error {
	// force pattern to be valid once cleaned
	pattern = path.Clean(pattern)
	if _, err := path.Match(pattern, ""); err != nil {
		return err
	}
	// the constant prefix in pattern
	// must match seek
	pre := MetaPrefix(pattern)
	seek = path.Clean(seek)
	if seek == "." {
		seek = ""
	}
	if seek != "" && (!strings.HasPrefix(seek, pre) || seek < pre) {
		return fmt.Errorf("seek %q doesn't match pattern %q", seek, pattern)
	}
	if ogs, ok := f.(WalkGlobFS); ok {
		return ogs.WalkGlob(seek, pattern, walk)
	}
	outer := func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return walk(p, nil, err)
		}
		if d.IsDir() {
			if p < seek && p != "." && !strings.HasPrefix(seek, p) {
				return fs.SkipDir
			}
			return nil
		}
		if p <= seek {
			return nil
		}
		match, err := path.Match(pattern, p)
		if err != nil || !match {
			return err
		}
		f, err := f.Open(p)
		if err != nil {
			return walk(p, nil, err)
		}
		return walk(p, f, nil)
	}
	if pre == "" {
		pre = "."
	}
	return fs.WalkDir(f, pre, outer)
}

// MetaPrefix finds the longest directory path for
// which we can begin searching for a glob pattern.
func MetaPrefix(pattern string) string {
	j := 0
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '*', '?', '\\', '[':
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
