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
	"testing"
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
