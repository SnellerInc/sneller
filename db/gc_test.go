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

package db

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/SnellerInc/sneller/ion/blockfmt"
)

func TestGC(t *testing.T) {
	checkFiles(t)
	tmpdir := t.TempDir()
	for _, dir := range []string{
		filepath.Join(tmpdir, "a-prefix/foo"),
	} {
		err := os.MkdirAll(dir, 0750)
		if err != nil {
			t.Fatal(err)
		}
	}
	newname := filepath.Join(tmpdir, "a-prefix/foo/parking.10n")
	oldname, err := filepath.Abs("../testdata/parking.10n")
	if err != nil {
		t.Fatal(err)
	}
	err = os.Symlink(oldname, newname)
	if err != nil {
		t.Fatal(err)
	}

	dfs := newDirFS(t, tmpdir)
	err = WriteDefinition(dfs, "default", &Definition{
		Name: "parking",
		Inputs: []Input{
			{Pattern: "file://a-prefix/{pre}/*.10n"},
		},
		Partitions: []Partition{
			{Field: "pre"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	bogus := []string{
		"db/default/parking/inputs-0000",
		"db/default/parking/inputs-1000",
		"db/default/parking/foo/packed-deleteme0.ion.zst",
		"db/default/parking/foo/packed-deleteme1.ion.zst",
	}
	for _, x := range bogus {
		_, err := dfs.WriteFile(x, []byte{})
		if err != nil {
			t.Fatal(err)
		}
	}

	owner := newTenant(dfs)
	c := Config{
		Align: 1024,
		Fallback: func(_ string) blockfmt.RowFormat {
			return blockfmt.UnsafeION()
		},
		Logf: t.Logf,
	}
	err = c.Sync(owner, "default", "*")
	if err != nil {
		t.Fatal(err)
	}
	// test that a second Sync determines
	// that everything is up-to-date and does nothing
	owner.ro = true
	err = c.Sync(owner, "default", "*")
	if err != nil {
		t.Fatal(err)
	}
	owner.ro = false
	idx, err := OpenIndex(dfs, "default", "parking", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	conf := GCConfig{Logf: t.Logf, MinimumAge: 1, InputMinimumAge: 1}
	err = conf.Run(dfs, "default", idx)
	if err != nil {
		t.Fatal(err)
	}
	// make sure all the objects pointed to
	// by the index still exist, and all the bogus
	// objects have been removed
	for i := range idx.Inline {
		p := idx.Inline[i].Path
		_, err := fs.Stat(dfs, p)
		if err != nil {
			t.Fatal(err)
		}
	}
	idx.Inputs.Backing = dfs
	idx.Inputs.EachFile(func(name string) {
		_, err := fs.Stat(dfs, name)
		if err != nil {
			t.Fatal(err)
		}
	})
	for i := range bogus {
		_, err := fs.Stat(dfs, bogus[i])
		if err == nil {
			t.Errorf("path %s: still exists?", bogus[i])
		} else if !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("path %s: unexpected error %v", bogus[i], err)
		}
	}
}
