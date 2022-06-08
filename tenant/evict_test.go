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

package tenant

import (
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"golang.org/x/exp/slices"
)

func TestEvict(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("this doesn't work on windows")
	}

	oldusage, oldatime := usage, atime
	t.Cleanup(func() {
		usage = oldusage
		atime = oldatime
	})
	tmp := t.TempDir()

	type fsent struct {
		name  string
		size  int64
		atime int64
	}

	base := time.Now().UnixNano()
	begin := []fsent{
		// total size is 2000/2000 in the starting state;
		// target will be 1800/2000 which means removing
		// the two oldest files (000 and 005)
		{"0/00", 100, base + 100},
		{"0/01", 100, base + 200},
		{"0/02", 100, base + 300},
		{"0/03", 100, base + 300},
		{"1/04", 1500, base + 500},
		{"1/05", 100, base - 200},
	}
	// the end state should just be the start state
	// minus the oldest file (which is listed first)
	end := begin[1:5]

	myUsage := func(dir string) (int64, int64) {
		sum := int64(0)
		err := filepath.WalkDir(dir, func(_ string, info fs.DirEntry, err error) error {
			if err != nil {
				t.Fatal(err)
			}
			if info.Type().IsRegular() {
				inode, err := info.Info()
				if err != nil {
					return err
				}
				sum += inode.Size()
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return sum, 2000
	}
	myAtime := func(i fs.FileInfo) int64 {
		name := i.Name()
		for i := range begin {
			if strings.HasSuffix(begin[i].name, name) {
				return begin[i].atime
			}
		}
		t.Fatal("unknown file name", name)
		return 0
	}

	usage = myUsage
	atime = myAtime

	// populate the tmpdir
	for i := range begin {
		fullpath := filepath.Join(tmp, begin[i].name)
		contents := []byte(strings.Repeat("a", int(begin[i].size)))
		os.MkdirAll(filepath.Dir(fullpath), 0755)
		err := ioutil.WriteFile(fullpath, contents, 0644)
		if err != nil {
			t.Fatal(err)
		}
	}

	readall := func(dir string) []string {
		var final []string
		err := filepath.WalkDir(dir, func(p string, info fs.DirEntry, err error) error {
			if err != nil {
				println("WalkDir error", err.Error())
				return err
			}
			if info.Type().IsRegular() {
				final = append(final, strings.TrimPrefix(p, dir)[1:])
			}
			return nil
		})
		if err != nil {
			t.Helper()
			t.Fatal(err)
		}
		return final
	}

	m := NewManager([]string{"/bin/false"})
	m.CacheDir = tmp
	// we only need two items buffered
	// to pick the right ones, so let's
	// exercise that path:
	m.eheap.maxbuffer = 2
	m.cacheEvict()

	final := readall(tmp)
	if len(final) != len(end) {
		t.Fatalf("%v remaining?", final)
	}
	// both 'final' and 'end' are sorted
	for i := range final {
		e := &end[i]
		if e.name != final[i] {
			t.Errorf("expected %s found %s", e.name, final[i])
		}
	}

	if !slices.IsSortedFunc(m.eheap.sorted, func(x, y fprio) bool {
		return x.atime < y.atime
	}) {
		t.Error("heap.sorted not sorted")
	}

	// since we've satisfied the usage criteria,
	// a second call to cacheEvict() shouldn't try
	// to remove anything
	m.cacheEvict()
	final = readall(tmp)
	if len(final) != len(end) {
		t.Fatalf("%d files remaining?", len(final))
	}
}

// test that stale atime entries
// do not lead to an infinite loop
func TestIssue645(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("this doesn't work on windows")
	}

	oldusage, oldatime := usage, atime
	t.Cleanup(func() {
		usage = oldusage
		atime = oldatime
	})
	tmp := t.TempDir()

	type fsent struct {
		name  string
		size  int64
		atime int64
	}

	base := time.Now().UnixNano()
	begin := []fsent{
		// total size is 940/1000 in the starting state
		{"000", 500, base + 100},
		{"001", 100, base + 200},
		{"002", 100, base + 300},
		{"003", 120, base + 300},
		{"004", 120, base + 500},
	}
	// the end state should just be the start state
	// minus the oldest file (which is listed first)
	end := begin[1:]

	myUsage := func(dir string) (int64, int64) {
		if dir != tmp {
			t.Fatal("bad tmpdir", dir)
		}
		contents, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		sum := int64(0)
		for i := range contents {
			fi, err := contents[i].Info()
			if err != nil {
				t.Fatal(err)
			}
			sum += fi.Size()
		}
		return sum, 1000
	}
	myAtime := func(i fs.FileInfo) int64 {
		name := i.Name()
		for i := range begin {
			if begin[i].name == name {
				return begin[i].atime
			}
		}
		t.Fatal("unknown file name", name)
		return 0
	}

	usage = myUsage
	atime = myAtime

	// populate the tmpdir
	for i := range begin {
		fullpath := filepath.Join(tmp, begin[i].name)
		contents := []byte(strings.Repeat("a", int(begin[i].size)))
		err := ioutil.WriteFile(fullpath, contents, 0644)
		if err != nil {
			t.Fatal(err)
		}
	}

	m := NewManager([]string{"/bin/false"})
	m.CacheDir = tmp
	m.cacheEvict()

	final, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatal(err)
	}
	if len(final) != len(end) {
		t.Fatalf("%d files remaining?", len(final))
	}
	// both 'final' and 'end' are sorted
	for i := range final {
		e := &end[i]
		if e.name != final[i].Name() {
			t.Errorf("expected %s found %s", e.name, final[i].Name())
		}
	}
	if len(m.eheap.sorted) != len(final) {
		t.Errorf("%d entries in sorted heap but %d final dirents?", len(m.eheap.sorted), len(final))

	}

	// check that sorted really means sorted;
	// each element's atime should be less than the next
	for i := range m.eheap.sorted[:len(m.eheap.sorted)-1] {
		j := i + 1
		if m.eheap.sorted[i].atime > m.eheap.sorted[j].atime {
			t.Errorf("heap.sorted[%d] > heap.sorted[%d]", i, j)
		}
	}

	// invalidate the atimes in the heap
	for i := range end {
		end[i].atime++
	}

	// re-populate the tmpdir
	for i := range begin {
		fullpath := filepath.Join(tmp, begin[i].name)
		contents := []byte(strings.Repeat("a", int(begin[i].size)))
		err := ioutil.WriteFile(fullpath, contents, 0644)
		if err != nil {
			t.Fatal(err)
		}
	}
	// run a second eviction; we should
	// get the same result as before, even
	// though some of the atimes are stale
	m.cacheEvict()
	if len(m.eheap.sorted) != len(final) {
		t.Errorf("second call to cacheEvict removed %d entries?", len(final)-len(m.eheap.sorted))
	}

}
