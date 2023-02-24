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
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/exp/slices"
)

func TestWalkGlob(t *testing.T) {
	dirs := []string{
		"a/b/c",
		"x/b/c",
		"x/y/a",
		"x/y/z",
	}

	cases := []struct {
		seek, pattern string
		results       []string
	}{
		{"", "x/?/?", []string{"x/b/c", "x/y/a", "x/y/z"}},
		{"x/y", "?/?/?", []string{"x/y/a", "x/y/z"}},
		{"x/y", "x/*y/*", []string{"x/y/a", "x/y/z"}},
		{"x", "x/?/?", []string{"x/b/c", "x/y/a", "x/y/z"}},
		{"x/y/a", "?/?/?", []string{"x/y/z"}},
		{"x/c", "?/?/?", []string{"x/y/a", "x/y/z"}},
		{"x/b/z", "?/?/?", []string{"x/y/a", "x/y/z"}},
	}
	tmp := t.TempDir()
	for _, full := range dirs {
		f := filepath.Clean(full)
		dir, _ := filepath.Split(f)
		err := os.MkdirAll(filepath.Join(tmp, dir), 0750)
		if err != nil {
			t.Fatal(err)
		}
		err = os.WriteFile(filepath.Join(tmp, f), []byte{}, 0640)
		if err != nil {
			t.Fatal(err)
		}
	}
	d := os.DirFS(tmp)
	for i := range cases {
		seek := cases[i].seek
		pattern := cases[i].pattern
		want := cases[i].results

		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			var got []string
			err := WalkGlob(d, seek, pattern, func(p string, f fs.File, err error) error {
				if err != nil {
					t.Fatal(err)
				}
				f.Close()
				got = append(got, p)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(want, got) {
				t.Errorf("want %v got %v", want, got)
			}
		})
	}
}

func TestOpenGlob(t *testing.T) {
	dirs := []string{
		"a/b/c",
		"x/b/z",
	}
	tmp := t.TempDir()
	for _, full := range dirs {
		f := filepath.Clean(full)
		dir, _ := filepath.Split(f)
		err := os.MkdirAll(filepath.Join(tmp, dir), 0750)
		if err != nil {
			t.Fatal(err)
		}
		err = os.WriteFile(filepath.Join(tmp, f), []byte{}, 0640)
		if err != nil {
			t.Fatal(err)
		}
	}
	d := os.DirFS(tmp)
	fn, err := OpenGlob(d, "[ax]/b/[cz]")
	if err != nil {
		t.Fatal(err)
	}
	if len(fn) != len(dirs) {
		t.Fatalf("got %d entries?", len(fn))
	}
	if fn[0].Path() != "a/b/c" {
		t.Errorf("path[0] = %q", fn[0].Path())
	}
	if fn[1].Path() != "x/b/z" {
		t.Errorf("path[1] = %q", fn[1].Path())
	}
	for i := range fn {
		err := fn[i].Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

// test that WalkGlob doesn't do too many
// unnecessary call as it walks a file tree.
func TestWalkGlobOps(t *testing.T) {
	// file tree to create
	list := []string{
		"a/",
		"a/b",
		"a/c",
		"a/d/",
		"a/e",
		"b/",
		"b/c",
		"b/d",
		"b/e/",
		"b/f/",
	}
	// create test files
	tmp := t.TempDir()
	for i := range list {
		if strings.HasSuffix(list[i], "/") {
			name := strings.TrimSuffix(list[i], "/")
			err := os.Mkdir(filepath.Join(tmp, name), 0750)
			if err != nil {
				t.Fatalf("creating dir %q: %v", name, err)
			}
		} else {
			err := os.WriteFile(filepath.Join(tmp, list[i]), []byte{}, 0640)
			if err != nil {
				t.Fatalf("creating file %q: %v", list[i], err)
			}
		}
	}
	tfs := &traceFS{fs: os.DirFS(tmp)}
	var got []string
	err := WalkGlob(tfs, "", "*/*", func(p string, f fs.File, err error) error {
		if err != nil {
			return err
		}
		f.Close()
		got = append(got, p)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"a/b",
		"a/c",
		"a/e",
		"b/c",
		"b/d",
	}
	if !slices.Equal(want, got) {
		t.Errorf("walked files: want %q, got %q", want, got)
	}
	wantops := []string{
		"open(.)",
		"visitdir(.)",
		"visitdir(a)",
		"open(a/b)",
		"open(a/c)",
		"open(a/e)",
		"visitdir(b)",
		"open(b/c)",
		"open(b/d)",
	}
	if !slices.Equal(wantops, tfs.ops) {
		t.Errorf("ops mismatch:")
		t.Errorf("  want: %q", wantops)
		t.Errorf("  got:  %q", tfs.ops)
	}
}

type traceFS struct {
	fs  fs.FS
	ops []string
}

func (f *traceFS) Open(name string) (fs.File, error) {
	f.logf("open(%s)", name)
	return f.fs.Open(name)
}

func (f *traceFS) VisitDir(name, seek, pattern string, fn VisitDirFn) error {
	f.logf("visitdir(%s)", name)
	return VisitDir(f.fs, name, seek, pattern, fn)
}

func (f *traceFS) logf(fm string, args ...any) {
	f.ops = append(f.ops, fmt.Sprintf(fm, args...))
}
