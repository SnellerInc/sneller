// Copyright 2023 Sneller, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package fsutil

import (
	"fmt"
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

// WalkGlob opens all of the non-directory
// files in f that match pattern.
// The seek parameter determines the full path
// at which walking begins, and pattern indicates
// the glob pattern against which file paths are matched
// before being passed to the walk callback.
//
// WalkGlob uses WalkDir to walk the file tree.
// If it encounters an fs.DirEntry that implements
// fs.File, it will be passed to walk directly.
// Otherwise, fs.Open will be used.
func WalkGlob(f fs.FS, seek, pattern string, walk WalkGlobFn) error {
	if pattern == "" {
		return fmt.Errorf("fsutil.WalkGlob: pattern is required")
	}
	pre := MetaPrefix(pattern)
	outer := func(p string, d DirEntry, err error) error {
		if err != nil {
			return walk(p, nil, err)
		}
		if d.IsDir() {
			return nil
		}
		if f, ok := d.(fs.File); ok {
			return walk(p, f, nil)
		}
		file, err := f.Open(p)
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
