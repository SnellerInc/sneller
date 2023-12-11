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
		filepath.Join(tmpdir, "a-prefix/bar"),
	} {
		err := os.MkdirAll(dir, 0750)
		if err != nil {
			t.Fatal(err)
		}
	}
	oldname, err := filepath.Abs("../testdata/parking.10n")
	if err != nil {
		t.Fatal(err)
	}
	for _, newname := range []string{
		"a-prefix/foo/parking.10n",
		"a-prefix/bar/parking.10n",
	} {
		err = os.Symlink(oldname, filepath.Join(tmpdir, newname))
		if err != nil {
			t.Fatal(err)
		}
	}

	dfs := newDirFS(t, tmpdir)
	err = WriteDefinition(dfs, "default", "parking", &Definition{
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
		"db/default/parking/bar/packed-deleteme0.ion.zst",
		"db/default/parking/foo/packed-deleteme1.ion.zst",
		"db/default/parking/bar/packed-deleteme1.ion.zst",
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
	conf := GCConfig{
		Logf:            t.Logf,
		MinimumAge:      1,
		InputMinimumAge: 1,
		MaxDelay:        1,
	}
	idx, err := OpenIndex(dfs, "default", "parking", owner.Key())
	if err != nil {
		t.Fatal(err)
	}
	err = conf.Run(dfs, "default", idx)
	if !errors.Is(err, errLongGC) {
		t.Errorf("first run: got %v", err)
	}
	// should have a non-empty cursor
	// since we stopped early:
	cursor := getPackedCursor(idx)
	if cursor == "" {
		t.Errorf("cursor = %s?", cursor)
	}
	err = conf.Run(dfs, "default", idx)
	if err != nil {
		t.Fatal(err)
	}
	// cursor should be empty now:
	cursor = getPackedCursor(idx)
	if cursor != "" {
		t.Errorf("cursor = %s?", cursor)
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
