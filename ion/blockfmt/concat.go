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
)

// Concat wraps a list of Descriptors
// for objects that should be concatenated.
//
// The zero value of Concat is a valid empty
// list of object to be concatenated.
// Objects can be added to the list with Add,
// and then the result can be written out
// with a call to Run.
type Concat struct {
	inputs []Descriptor
	output Descriptor
}

// Add returns true if src was added to the
// list of objects to concatenate, or false
// if the descriptor is not compatible with
// the descriptors that have already been
// added to the collection list.
//
// Descriptors are "compatible" if they encode
// data in precisely the same way *and* their
// sparse indexing metadata covers the same
// constants and time ranges (see SparseIndex.Append)
func (c *Concat) Add(src *Descriptor) bool {
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

// Len returns the number of objects to be concatenated
func (c *Concat) Len() int { return len(c.inputs) }

// Result returns the final descriptor for
// the concatenated objects. Result is only
// valid if Run has been called and returned
// without an error.
func (c *Concat) Result() Descriptor {
	return c.output
}

// Run concatenates all the descriptors added via Add
// into the file given by name in the provided filesystem.
func (c *Concat) Run(fs UploadFS, name string) error {
	if len(c.inputs) == 0 {
		return fmt.Errorf("blockfmt.Concat.Run with zero input objects")
	}
	up, err := fs.Create(name)
	if err != nil {
		return err
	}
	c.output.Path = name

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
			return fmt.Errorf("blockfmt.Concat.Run: etag mismatch for %s (%s -> %s)", c.inputs[i].Path, c.inputs[i].ETag, etag)
		}
		part, err = uploadReader(up, part, f, c.inputs[i].Trailer.Offset)
		if err != nil {
			f.Close()
			return err
		}
		err = f.Close()
		if err != nil {
			return err
		}
	}
	tail := c.output.Trailer.trailer(c.output.Trailer.Algo, 1<<c.output.Trailer.BlockShift)
	err = up.Close(tail)
	if err != nil {
		return err
	}
	c.output.ETag, err = ETag(fs, up, c.output.Path)
	c.output.Size = up.Size()
	return err
}
