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
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func (t *totalHeap) count() int {
	return t.buffered.count()
}

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
		// the two oldest files in the heaviest tenant (0/00 and 0/01)
		// and also the ephemeral file from tenant 1
		{"0/00", 100, base + 100},
		{"0/01", 100, base + 200},
		{"0/02", 100, base + 300},
		{"0/03", 100, base + 300},
		{"0/04", 1400, base + 500},
		{"1/05", 100, base - 200},
		{"1/eph:06", 100, base - int64(7*time.Second)},
	}
	// the end state should just be the start state
	// minus the oldest file (which is listed first)
	end := begin[2 : len(begin)-1]

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
		err := os.WriteFile(fullpath, contents, 0644)
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
		t.Fatalf("%v remaining", final)
	}
	// both 'final' and 'end' are sorted
	for i := range final {
		e := &end[i]
		if e.name != final[i] {
			t.Errorf("expected %s found %s", e.name, final[i])
		}
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
		// total size is 940/1000 in the starting state;
		// the heaviest tenant is 00, so its oldest file (00/000)
		// will be removed first
		{"00/000", 500, base + 100},
		{"00/001", 100, base + 200},
		{"01/002", 100, base + 300},
		{"01/003", 120, base + 300},
		{"02/004", 120, base + 000},
	}
	// the end state should just be the start state
	// minus the oldest file (which is listed first)
	end := begin[1:]

	myUsage := func(dir string) (int64, int64) {
		if dir != tmp {
			t.Fatal("bad tmpdir", dir)
		}
		total := int64(0)
		err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.Type().IsRegular() {
				fi, err := d.Info()
				if err != nil {
					t.Fatal(err)
				}
				total += fi.Size()
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return total, 1000
	}
	myAtime := func(i fs.FileInfo) int64 {
		name := i.Name()
		for i := range begin {
			if filepath.Base(begin[i].name) == name {
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
		os.MkdirAll(filepath.Dir(fullpath), 0755)
		contents := []byte(strings.Repeat("a", int(begin[i].size)))
		err := os.WriteFile(fullpath, contents, 0644)
		if err != nil {
			t.Fatal(err)
		}
	}

	m := NewManager([]string{"/bin/false"})
	m.CacheDir = tmp
	m.cacheEvict()

	final, err := filepath.Glob(filepath.Join(tmp, "*/???"))
	if err != nil {
		t.Fatal(err)
	}
	if len(final) != len(end) {
		t.Fatalf("%d files remaining? (expected %d)", len(final), len(end))
	}
	// both 'final' and 'end' are sorted
	pre := tmp + "/"
	for i := range final {
		e := &end[i]
		want := strings.TrimPrefix(final[i], pre)
		if e.name != want {
			t.Errorf("expected %s found %s", e.name, want)
		}
	}
	if len(m.eheap.sorted) != len(final) {
		t.Errorf("%d entries in sorted heap but %d final dirents?", m.eheap.count(), len(final))
	}

	// invalidate the atimes in the heap
	for i := range end {
		end[i].atime++
	}

	// re-populate the tmpdir
	for i := range begin {
		fullpath := filepath.Join(tmp, begin[i].name)
		contents := []byte(strings.Repeat("a", int(begin[i].size)))
		err := os.WriteFile(fullpath, contents, 0644)
		if err != nil {
			t.Fatal(err)
		}
	}
	// run a second eviction; we should
	// get the same result as before, even
	// though some of the atimes are stale
	m.cacheEvict()
	if len(m.eheap.sorted) != len(final) {
		t.Errorf("second call to cacheEvict removed %d entries?", len(final)-m.eheap.count())
	}

}
