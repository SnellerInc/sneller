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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"slices"
)

func TestVisitDir(t *testing.T) {
	// must be sorted
	list := []string{
		"a.txt",
		"b.txt",
		"c.txt",
		"foo",
		"z.txt",
	}
	tmp := t.TempDir()
	for i := range list {
		err := os.WriteFile(filepath.Join(tmp, list[i]), []byte{}, 0640)
		if err != nil {
			t.Fatalf("creating file %q: %v", list[i], err)
		}
	}
	cases := []struct {
		seek, pattern string
	}{
		{"", ""},
		{"c.txt", ""},
		{"", "*.txt"},
		{"", "foo"},
		{"foo", "*.txt"},
	}
	// trivial implementation
	trivial := func(seek, pattern string) []string {
		var out []string
		for i := range list {
			if list[i] <= seek {
				continue
			}
			if pattern != "" {
				m, err := path.Match(pattern, list[i])
				if err != nil {
					t.Fatal(err)
				}
				if !m {
					continue
				}
			}
			out = append(out, list[i])
		}
		return out
	}
	dir := os.DirFS(tmp)
	for i := range cases {
		seek := cases[i].seek
		pattern := cases[i].pattern
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			var got []string
			err := VisitDir(dir, ".", seek, pattern, func(d DirEntry) error {
				got = append(got, d.Name())
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			want := trivial(seek, pattern)
			if !reflect.DeepEqual(want, got) {
				t.Errorf("walk(%q, %q) mismatch:", seek, pattern)
				t.Errorf("  want: %q", want)
				t.Errorf("  got:  %q", got)
			}
		})
	}
}

// trivialWalkDir trivially implements the
// behavior of WalkDir without the added
// benefits of plumbing seek and pattern down to
// the directory listing code.
//
// This is the behavior we want to ensure that
// WalkDir correctly implements.
func trivialWalkDir(f fs.FS, name, seek, pattern string, fn WalkDirFn) error {
	return fs.WalkDir(f, name, func(p string, d fs.DirEntry, err error) error {
		if pattern != "" {
			match, err := path.Match(pattern, p)
			if err != nil || !match {
				return err
			}
		}
		if pathcmp(p, seek) > 0 {
			return fn(p, d, err)
		}
		return err
	})
}

// walkDirFn is any function with a signature
// like WalkDir.
type walkDirFn func(f fs.FS, name, seek, pattern string, fn WalkDirFn) error

// flatwalk returns all walked paths in a list.
func flatwalk(walkdir walkDirFn, f fs.FS, name, seek, pattern string, limit uint) ([]string, error) {
	var out []string
	err := walkdir(f, name, seek, pattern, func(p string, d DirEntry, err error) error {
		if limit > 0 && uint(len(out)) >= limit {
			panic("fs.SkipAll did not work as expected")
		}
		if err != nil {
			return err
		}
		out = append(out, p)
		if limit > 0 && uint(len(out)) >= limit {
			return fs.SkipAll
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func FuzzWalkDir(f *testing.F) {
	// file tree to create
	list := []string{
		"a",
		"a/b",
		"a/b/c",
		"a/b/d.txt",
		"b",
		"b/c",
		"b/d",
		"b/d/e",
		"b/e",
		"b/e/f",
		"b/e/f/g",
		"b/e/f/g/h.txt",
		"b/e/g",
		"b/f.txt",
		"c",
		"c/d",
		"c/d/e",
		"c/e",
		"c/f.txt",
		"d",
		"d.txt",
	}
	// seeks to test
	seeks := append(list, []string{
		"",
		"*",
		".",
		"a/z",
		"b/c/d/e/f",
		"blah",
		"e.txt",
		"foo/bar",
		"z",
	}...)
	// patterns to test
	patterns := []string{
		"",
		"*",
		"*/*",
		"a/*",
		"*/[ac]",
		"b/e/f/g/*.txt",
	}
	// create test files
	tmp := f.TempDir()
	for i := range list {
		if !strings.Contains(list[i], ".") {
			err := os.Mkdir(filepath.Join(tmp, list[i]), 0750)
			if err != nil {
				f.Fatalf("creating dir %q: %v", list[i], err)
			}
		} else {
			err := os.WriteFile(filepath.Join(tmp, list[i]), []byte{}, 0640)
			if err != nil {
				f.Fatalf("creating file %q: %v", list[i], err)
			}
		}
	}
	dir := os.DirFS(tmp)
	for _, seek := range seeks {
		for _, pattern := range patterns {
			f.Add(seek, pattern, uint(0))
			f.Add(seek, pattern, uint(10))
		}
	}
	validate := func(seek, pattern string) bool {
		if seek != "" {
			if !fs.ValidPath(seek) {
				return false
			}
			// make sure path is not rejected by the
			// file system
			f, err := dir.Open(seek)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				return false
			} else if err == nil {
				f.Close()
			}
		}
		if pattern != "" && !fs.ValidPath(pattern) {
			return false
		}
		// ignore patterns like "f[o/]o" for now...
		for n := 1; ; n++ {
			p, rest, ok := trim(pattern, n)
			if !ok {
				return false
			}
			if _, err := path.Match(p, ""); err != nil {
				return false
			}
			if rest == "" {
				break
			}
		}
		return true
	}
	f.Fuzz(func(t *testing.T, seek, pattern string, limit uint) {
		// ignore invalid arguments
		if !validate(seek, pattern) {
			t.Skipf("skipping invalid arguments seek=%q pattern=%q", seek, pattern)
		}
		for i, name := range list {
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				got, err := flatwalk(WalkDir, dir, name, seek, pattern, limit)
				if err != nil {
					t.Fatalf("WalkDir(%q, %q, %q, %q) returned %v", dir, name, seek, pattern, err)
				}
				want, err := flatwalk(trivialWalkDir, dir, name, seek, pattern, limit)
				if err != nil {
					t.Fatalf("trivialWalkDir(%q, %q, %q, %q) returned %v", dir, name, seek, pattern, err)
				}
				if !reflect.DeepEqual(want, got) {
					t.Errorf("walk(%q, %q, %q, %d) mismatch:", name, seek, pattern, limit)
					t.Errorf("  want: %q", want)
					t.Errorf("  got:  %q", got)
				}
			})
		}
	})
}

func FuzzSegments(f *testing.F) {
	trivial := func(p string) (int, bool) {
		if p == "" || p == "." {
			return 0, true
		}
		if !fs.ValidPath(p) {
			return 0, false
		}
		return strings.Count(p, "/") + 1, true
	}
	for _, s := range []string{
		"",
		".",
		"..",
		"/",
		"a",
		"a/b",
		"a/b/c",
		"foo",
		"foo/bar",
		"foo/bar/baz",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, p string) {
		got, gotok := segments(p)
		want, wantok := trivial(p)
		if got != want || gotok != wantok {
			t.Errorf("segments(%q): want (%d, %v), got (%d, %v)", p, want, wantok, got, gotok)
		}
	})
}

func FuzzTrim(f *testing.F) {
	trivial := func(p string, n int) (front, next string, ok bool) {
		if p != "" && !fs.ValidPath(p) {
			return "", "", false
		}
		if p == "" || p == "." {
			if n == 0 {
				return "", p, true
			}
			return p, "", true
		}
		join := func(ps []string) string {
			return path.Join(ps...)
		}
		ps := strings.Split(p, "/")
		if len(ps) <= n {
			return join(ps), "", true
		}
		return join(ps[:n]), ps[n], true
	}
	paths := []string{
		"",
		"*",
		".",
		"a",
		"a/*",
		"a/*/c",
		"a/?/*.txt",
		"a/b",
		"a/b/c",
		"foo/bar/baz/quux",
	}
	for _, p := range paths {
		for n := uint(0); n < 10; n++ {
			f.Add(p, n)
		}
	}
	f.Fuzz(func(t *testing.T, p string, n uint) {
		f0, n0, ok0 := trivial(p, int(n))
		f1, n1, ok1 := trim(p, int(n))
		if f0 != f1 || n0 != n1 || ok0 != ok1 {
			t.Errorf("trim(%q, %d): want (%q, %q, %v), got (%q, %q, %v)", p, n, f0, n0, ok0, f1, n1, ok1)
		}
	})
}

func FuzzPathcmp(f *testing.F) {
	trivial := func(a, b string) int {
		if a == "." {
			a = ""
		}
		if b == "." {
			b = ""
		}
		as := strings.Split(a, "/")
		bs := strings.Split(b, "/")
		return slices.Compare(as, bs)
	}
	cases := []struct {
		a, b string
	}{
		{"", ""},
		{"", "."},
		{".", "."},
		{".", "a"},
		{"a", "."},
		{"a", "a/b"},
		{"a/b", "."},
		{"a/b/c", "a/b"},
		{"foo/bar", "a/b"},
	}
	for i := range cases {
		f.Add(cases[i].a, cases[i].b)
	}
	f.Fuzz(func(t *testing.T, a, b string) {
		test := func(a, b string) {
			t.Helper()
			got := pathcmp(a, b)
			want := trivial(a, b)
			if got != want {
				t.Errorf("pathcmp(%q, %q): want %d, got %d", a, b, want, got)
			}
		}
		test(a, b)
		test(b, a)
	})
}

func FuzzTreecmp(f *testing.F) {
	trivial := func(root, p string) int {
		if root == "." {
			return 0
		}
		if p == "." {
			return -1
		}
		if root == p || strings.HasPrefix(p, root) && p[len(root)] == '/' {
			return 0
		}
		// make a file tree
		tree := []string{
			root,
			path.Join(root, "foo"),
			path.Join(root, "foo/bar"),
		}
		// insert p
		tree = append(tree, p)
		// sort it lexically
		slices.SortFunc(tree, func(a, b string) int {
			return pathcmp(a, b)
		})
		// look for p
		if tree[0] == p {
			return -1
		}
		if tree[len(tree)-1] == p {
			return 1
		}
		return 0
	}
	cases := []struct {
		a, b string
	}{
		{".", "."},
		{".", "a"},
		{"a", "a/b"},
		{"a/b", "."},
		{"a/b/c", "a/b"},
		{"foo/bar", "a/b"},
		{"c/e", "c/d/e"},
	}
	for i := range cases {
		f.Add(cases[i].a, cases[i].b)
	}
	f.Fuzz(func(t *testing.T, a, b string) {
		test := func(a, b string) {
			if !fs.ValidPath(a) || !fs.ValidPath(b) {
				return
			}
			t.Helper()
			got := treecmp(a, b)
			want := trivial(a, b)
			if got != want {
				t.Errorf("treecmp(%q, %q): want %d, got %d", a, b, want, got)
			}
		}
		test(a, b)
		test(b, a)
	})
}
