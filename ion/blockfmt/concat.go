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

package blockfmt

import (
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/SnellerInc/sneller/date"
)

// concat wraps a list of Descriptors
// for objects that should be concatenated.
//
// The zero value of concat is a valid empty
// list of object to be concatenated.
// Objects can be added to the list with add,
// and then the result can be written out
// with a call to run.
type concat struct {
	inputs []Descriptor
	output Descriptor
}

// add returns true if src was added to the
// list of objects to concatenate, or false
// if the descriptor is not compatible with
// the descriptors that have already been
// added to the collection list.
//
// Descriptors are "compatible" if they encode
// data in precisely the same way *and* their
// sparse indexing metadata covers the same
// constants and time ranges (see SparseIndex.Append)
func (c *concat) add(src *Descriptor) bool {
	t := &src.Trailer
	dt := &c.output.Trailer
	if len(c.inputs) == 0 {
		c.output.Format = src.Format
		dt.Algo = t.Algo
		dt.Version = t.Version
		dt.BlockShift = t.BlockShift
		dt.Sparse = t.Sparse.Clone()
	} else {
		dt := &c.output.Trailer
		// ensure trailer is compatible
		if t.Version != dt.Version ||
			t.Algo != dt.Algo ||
			t.BlockShift != dt.BlockShift ||
			!dt.Sparse.Append(&t.Sparse) {
			return false
		}
	}
	for i := range t.Blocks {
		dt.Blocks = append(dt.Blocks, Blockdesc{
			Offset: dt.Offset + t.Blocks[i].Offset,
			Chunks: t.Blocks[i].Chunks,
		})
	}
	c.inputs = append(c.inputs, *src)
	// dt.Offset is always the position immediately
	// following the final block of data
	dt.Offset += src.Trailer.Offset
	return true
}

func (c *concat) inputSize() int64 { return c.output.Trailer.Offset }

func (c *concat) result() Descriptor {
	return c.output
}

// run concatenates all the descriptors added via add
// into the file given by name in the provided filesystem.
func (c *concat) run(fs UploadFS, name string) error {
	if len(c.inputs) == 0 {
		return fmt.Errorf("blockfmt.concat.Run with zero input objects")
	}
	up, err := fs.Create(name)
	if err != nil {
		return err
	}
	c.output.Path = name

	var finalbuf []byte
	part := int64(1)
	for i := range c.inputs {
		f, err := fs.Open(c.inputs[i].Path)
		if err != nil {
			return err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		etag, err := fs.ETag(c.inputs[i].Path, info)
		if err != nil {
			f.Close()
			return err
		}
		if etag != c.inputs[i].ETag {
			f.Close()
			return fmt.Errorf("blockfmt.concat.Run: etag mismatch for %s (%s -> %s)", c.inputs[i].Path, c.inputs[i].ETag, etag)
		}
		if c.inputs[i].Trailer.Offset < int64(up.MinPartSize()) {
			if i != len(c.inputs)-1 {
				return fmt.Errorf("non-final object size %d below minimum part size %d", c.inputs[i].Trailer.Offset, up.MinPartSize())
			}
			// all but the final input must be above the minimum part size;
			// the final input can be smaller in which case we buffer it
			// and prepend it to the trailer
			finalbuf = make([]byte, c.inputs[i].Trailer.Offset)
			_, err = io.ReadFull(f, finalbuf)
			if err != nil {
				f.Close()
				return fmt.Errorf("concat last small object: %w", err)
			}
		} else {
			part, err = uploadReader(up, part, f, c.inputs[i].Trailer.Offset)
			if err != nil {
				f.Close()
				return err
			}
		}
		err = f.Close()
		if err != nil {
			return err
		}
	}
	tail := c.output.Trailer.trailer(c.output.Trailer.Algo, 1<<c.output.Trailer.BlockShift)
	if finalbuf != nil {
		tail = append(finalbuf, tail...)
	}
	err = up.Close(tail)
	if err != nil {
		return err
	}
	c.output.ETag, err = ETag(fs, up, c.output.Path)
	c.output.Size = up.Size()
	return err
}

func suffixForComp(c string) string {
	if c == "zstd" {
		return ".ion.zst"
	}
	if strings.HasPrefix(c, "zion") {
		return ".zion"
	}
	panic("bad suffixForComp value")
	return ""
}

// Compact compacts a list of descriptors and returns a new
// (hopefully shorter) list of descriptors containing the same data
// along with the list of quarantined descriptor paths that should
// be deleted.
func (c *IndexConfig) Compact(fs UploadFS, lst []Descriptor) ([]Descriptor, []Quarantined, error) {
	target := c.TargetSize
	expiry := date.Now().Truncate(time.Microsecond).Add(c.Expiry)
	if len(lst) == 1 {
		return lst, nil, nil
	}
	paths := make(map[string]*concat)

	var result []Descriptor
	var todelete []Quarantined
	var lock sync.Mutex
	var wg sync.WaitGroup
	errc := make(chan error, 1)

	// add d to result and add old to todelete (if any),
	// taking care to synchronize against other replace() calls
	replace := func(d Descriptor, old []Descriptor) {
		lock.Lock()
		defer lock.Unlock()
		result = append(result, d)
		for i := range old {
			todelete = append(todelete, Quarantined{
				Path:   old[i].Path,
				Expiry: expiry,
			})
		}
	}

	// wait for async operations to complete
	// and produce the first error encountered (if any)
	wait := func() error {
		go func() {
			wg.Wait()
			close(errc)
		}()
		var err error
		for e := range errc {
			if err == nil {
				err = e
			}
		}
		return err
	}

	// begin an async concatenation operation
	flush := func(c *concat, dir string) {
		if len(c.inputs) == 0 {
			return
		}
		if len(c.inputs) == 1 {
			replace(c.inputs[0], nil)
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			orig := c.inputs
			err := c.run(fs, path.Join(dir, "packed-"+uuid()+suffixForComp(orig[0].Trailer.Algo)))
			if err != nil {
				errc <- err
				return
			}
			replace(c.result(), orig)
		}()
	}

	for i := range lst {
		if lst[i].Size >= target {
			replace(lst[i], nil)
			continue
		}
		dir, _ := path.Split(lst[i].Path)
		c := paths[dir]
		if c == nil {
			c = new(concat)
			paths[dir] = c
		}
		if !c.add(&lst[i]) {
			replace(lst[i], nil)
			continue
		}
		if c.inputSize() >= target {
			flush(c, dir)
			delete(paths, dir)
		}
	}
	for dir, c := range paths {
		flush(c, dir)
	}
	err := wait()
	return result, todelete, err
}
