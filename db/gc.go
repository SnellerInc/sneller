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
	"fmt"
	"io/fs"
	"path"
	"sync"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/fsutil"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// RemoveFS is an fs.FS with a Remove operation.
type RemoveFS interface {
	fs.FS
	Remove(name string) error
}

var (
	_ RemoveFS = &S3FS{}
	_ RemoveFS = &DirFS{}
)

const (
	// DefaultMinimumAge is the default minimum
	// age of packed-* files to be deleted.
	DefaultMinimumAge = 15 * time.Minute
	// DefaultInputMinimumAge is the default
	// minimuma ge of inputs-* files to be deleted.
	DefaultInputMinimumAge = 30 * time.Second
)

// GCConfig is a configuration for
// garbage collection.
type GCConfig struct {
	// MinimumAge, if non-zero, specifies
	// the minimum age for any objects removed
	// during a garbage-collection pass.
	// Note that objects are only candidates
	// for garbage collection if they are older
	// than the current index *and* not pointed to
	// by the current index, so the MinimumAge requirement
	// is only necessary if it is possible for GC and ingest
	// to run simultaneously. In that case, MinimumAge should be
	// set to some duration longer than any possible ingest cycle.
	MinimumAge      time.Duration
	InputMinimumAge time.Duration

	// Logf, if non-nil, is a callback used for logging
	// detailed information regarding GC decisions.
	Logf func(f string, args ...interface{})

	// Precise determines if GC is performed
	// by only deleting objects that have been
	// explicitly marked for deletion.
	Precise bool
}

func (c *GCConfig) logf(f string, args ...interface{}) {
	// let `go vet` know this is printf-like
	if false {
		_ = fmt.Sprintf(f, args...)
	}
	if c.Logf != nil {
		c.Logf(f, args...)
	}
}

type readOnly struct {
	blockfmt.InputFS
}

func (r *readOnly) WriteFile(_ string, _ []byte) (string, error) {
	return "", fmt.Errorf("WriteFile on read-only UploadFS")
}

func (r *readOnly) Create(_ string) (blockfmt.Uploader, error) {
	return nil, fmt.Errorf("Create on read-only UploadFS")
}

func (c *GCConfig) remove(rfs RemoveFS, p string) {
	err := rfs.Remove(p)
	if err == nil || errors.Is(err, fs.ErrNotExist) {
		c.logf("removed %s", p)
	} else {
		c.logf("removing %s: %s", p, err)
	}
}

func (c *GCConfig) runInputs(rfs RemoveFS, dir string, idx *blockfmt.Index, start time.Time, min time.Duration) error {
	used := make(map[string]struct{})
	ifs, ok := rfs.(blockfmt.InputFS)
	if !ok {
		return fmt.Errorf("cannot scan indirect inputs using %T", rfs)
	}
	idx.Inputs.Backing = &readOnly{ifs}
	idx.Inputs.EachFile(func(f string) {
		used[path.Base(f)] = struct{}{}
	})
	const pattern = "inputs-*"
	matches := func(p string) bool {
		ok, err := path.Match(pattern, p)
		return err == nil && ok
	}
	visit := func(d fsutil.DirEntry) error {
		name := d.Name()
		if d.IsDir() || !matches(name) {
			return nil
		}
		if _, ok := used[name]; ok {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			c.logf("%s: %v", name, err)
			// fine if stuff was removed before we stat'd it:
			if errors.Is(err, fs.ErrNotExist) {
				err = nil
			}
			return err
		}
		if start.Sub(info.ModTime()) < min {
			// not old enough
			return nil
		}
		c.remove(rfs, path.Join(dir, name))
		return nil
	}
	return fsutil.VisitDir(rfs, dir, "", pattern, visit)
}

func (c *GCConfig) runPacked(rfs RemoveFS, dir string, idx *blockfmt.Index, start time.Time, min time.Duration) error {
	used := make(map[string]struct{})
	subdirs := make(map[string]struct{})
	// we're cheating a bit: we know that packfile names
	// end in UUIDs, so just comparing against the basename
	// is enough to identify the file; we don't need to
	// record the complete path
	for i := range idx.Inline {
		subdir, name := path.Split(idx.Inline[i].Path)
		used[name] = struct{}{}
		subdirs[path.Clean(subdir)] = struct{}{}
	}
	ifs, ok := rfs.(blockfmt.InputFS)
	if !ok {
		return fmt.Errorf("cannot scan indirect inputs using %T", rfs)
	}
	descs, err := idx.Indirect.Search(ifs, nil)
	if err != nil {
		return err
	}
	for i := range descs {
		subdir, name := path.Split(descs[i].Path)
		used[name] = struct{}{}
		subdirs[path.Clean(subdir)] = struct{}{}
	}
	const pattern = "packed-*"
	for sub := range subdirs {
		matches := func(p string) bool {
			ok, err := path.Match(pattern, p)
			return err == nil && ok
		}
		visit := func(d fsutil.DirEntry) error {
			name := d.Name()
			if d.IsDir() || !matches(name) {
				return nil
			}
			if _, ok := used[name]; ok {
				return nil
			}
			info, err := d.Info()
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			} else if err != nil {
				c.logf("%s: %v", path.Join(sub, name), err)
				return err
			}
			if start.Sub(info.ModTime()) < min {
				return nil
			}
			c.remove(rfs, path.Join(sub, name))
			return nil
		}
		err := fsutil.VisitDir(rfs, sub, "", pattern, visit)
		if err != nil {
			return err
		}
	}
	return nil
}

// Run calls rfs.Remove(path) for each path
// within the provided database name and table
// that a) has a filename pattern that indicates
// it was packed by Sync, at b) is not pointed to
// by idx.
func (c *GCConfig) Run(rfs RemoveFS, dbname string, idx *blockfmt.Index) error {
	if c.Precise {
		c.preciseGC(rfs, idx)
	}
	start := time.Now()
	dir := path.Join("db", dbname, idx.Name)
	packedmin := c.MinimumAge
	if packedmin <= 0 {
		packedmin = DefaultMinimumAge
	}
	inputmin := c.InputMinimumAge
	if inputmin <= 0 {
		inputmin = DefaultInputMinimumAge
	}
	err := c.runInputs(rfs, dir, idx, start, inputmin)
	if err != nil {
		return fmt.Errorf("scanning inputs: %w", err)
	}
	err = c.runPacked(rfs, dir, idx, start, packedmin)
	if err != nil {
		return fmt.Errorf("scanning packfiles: %w", err)
	}
	return nil
}

// preciseGC removes expired elements from idx.ToDelete
// and returns true if any items were removed, or otherwise false
func (c *GCConfig) preciseGC(rfs RemoveFS, idx *blockfmt.Index) bool {
	if len(idx.ToDelete) == 0 {
		return false
	}
	saved := idx.ToDelete[:0]
	now := date.Now()
	var failed chan blockfmt.Quarantined
	var wg sync.WaitGroup
	for i := range idx.ToDelete {
		if idx.ToDelete[i].Expiry.After(now) {
			saved = append(saved, idx.ToDelete[i])
			continue
		}
		x := idx.ToDelete[i]
		if failed == nil {
			failed = make(chan blockfmt.Quarantined, 1)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := rfs.Remove(x.Path)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				c.logf("deleting ToDelete %q: %s", x.Path, err)
				failed <- x
			}
		}()
	}
	// didn't remove anything:
	if failed == nil {
		return false
	}
	go func() {
		wg.Wait()
		close(failed)
	}()
	for x := range failed {
		saved = append(saved, x)
	}
	idx.ToDelete = saved
	return true
}
