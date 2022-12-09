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
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/exp/slices"
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
			if list[i] < seek {
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
			err := VisitDir(dir, ".", seek, pattern, func(d fs.DirEntry) error {
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
func trivialWalkDir(f fs.FS, name, seek, pattern string, fn fs.WalkDirFunc) error {
	return fs.WalkDir(f, name, func(p string, d fs.DirEntry, err error) error {
		if pattern != "" {
			match, err := path.Match(pattern, p)
			if err != nil || !match {
				return err
			}
		}
		if pathcmp(p, seek) >= 0 {
			return fn(p, d, err)
		}
		return err
	})
}

// walkDirFn is any function with a signature
// like WalkDir.
type walkDirFn func(f fs.FS, name, seek, pattern string, fn fs.WalkDirFunc) error

// flatwalk returns all walked paths in a list.
func flatwalk(walkdir walkDirFn, f fs.FS, name, seek, pattern string) ([]string, error) {
	var out []string
	err := walkdir(f, name, seek, pattern, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		out = append(out, p)
		return nil
	})
	return out, err
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
			f.Add(seek, pattern)
		}
	}
	validate := func(seek, pattern string) bool {
		if seek != "" && !fs.ValidPath(seek) {
			return false
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
	f.Fuzz(func(t *testing.T, seek, pattern string) {
		// ignore invalid arguments
		if !validate(seek, pattern) {
			t.Skipf("skipping invalid arguments seek=%q pattern=%q", seek, pattern)
		}
		for _, name := range list {
			got, err := flatwalk(WalkDir, dir, name, seek, pattern)
			if err != nil {
				t.Fatalf("WalkDir(%q, %q, %q, %q) returned %v", dir, name, seek, pattern, err)
			}
			want, err := flatwalk(trivialWalkDir, dir, name, seek, pattern)
			if err != nil {
				t.Fatalf("trivialWalkDir(%q, %q, %q, %q) returned %v", dir, name, seek, pattern, err)
			}
			if !reflect.DeepEqual(want, got) {
				t.Errorf("walk(%q, %q, %q) mismatch:", name, seek, pattern)
				t.Errorf("  want: %q", want)
				t.Errorf("  got:  %q", got)
			}
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
