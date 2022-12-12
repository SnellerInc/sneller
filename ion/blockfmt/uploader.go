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
	"sort"
	"sync"

	"github.com/SnellerInc/sneller/aws/s3"
)

// Uploader describes what we expect
// an object store upload API to look like.
//
// (Take a look at aws/s3.Uploader.)
type Uploader interface {
	// MinPartSize is the minimum supported
	// part size for the Uploader.
	MinPartSize() int
	// Upload should upload contents
	// as the given part number.
	// Part numbers may be sparse, but
	// they will always be positive and non-zero.
	// Upload is not required to handle
	// len(contents) < MinPartSize().
	Upload(part int64, contents []byte) error
	// Close should append final to the
	// object contents and then finalize
	// the object. Close must handle
	// len(final) < MinPartSize().
	Close(final []byte) error
	// Size should return the final size
	// of the uploaded object. It is only
	// required to return a valid value
	// after Close has been called.
	Size() int64
}

func uploadReader(dst Uploader, startpart int64, src io.Reader, size int64) (int64, error) {
	if size < int64(dst.MinPartSize()) {
		return startpart, fmt.Errorf("cannot upload %d bytes from reader (less than min part size %d)", size, dst.MinPartSize())
	}
	// fast-path for the real world: just use S3 server-side copy
	if f, ok := src.(*s3.File); ok {
		if up, ok := dst.(*s3.Uploader); ok {
			start, _ := f.Seek(0, io.SeekCurrent)
			err := up.CopyFrom(startpart, &f.Reader, start, size)
			if err != nil {
				return startpart, err
			}
			// adjust the reader so that any subequent
			// bytes are read from the right place
			_, err = f.Seek(size, io.SeekCurrent)
			if err != nil {
				return startpart, err
			}
			return startpart + 1, nil
		}
	}
	var buffer []byte
	target := dst.MinPartSize()
	n := int64(0)
	for n < size {
		remaining := size - n
		amt := target
		if remaining < int64(2*amt) {
			amt = int(remaining)
		}
		if cap(buffer) >= amt {
			buffer = buffer[:amt]
		} else {
			buffer = make([]byte, amt)
		}
		_, err := io.ReadFull(src, buffer)
		if err != nil {
			return startpart, err
		}
		err = dst.Upload(startpart, buffer)
		if err != nil {
			return startpart, err
		}
		startpart++
		n += int64(amt)
	}
	return startpart, nil
}

// BufferUploader is a simple in-memory
// implementation of Uploader.
type BufferUploader struct {
	PartSize int
	lock     sync.Mutex
	partial  map[int64][]byte
	final    []byte
}

func (b *BufferUploader) Parts() int {
	if b.partial == nil {
		return 0
	}
	return len(b.partial)
}

// MinPartSize implements Uploader.PartSize
func (b *BufferUploader) MinPartSize() int {
	if b.PartSize == 0 {
		return 1 // don't ever allow empty parts
	}
	return b.PartSize
}

// Upload implements Uploader.Upload
func (b *BufferUploader) Upload(part int64, contents []byte) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	if len(contents) == 0 || len(contents) < b.PartSize {
		return fmt.Errorf("part %d: cannot be %d bytes (minimum %d)", part, len(contents), b.PartSize)
	}
	if b.partial == nil {
		b.partial = make(map[int64][]byte)
	} else if b.partial[part] != nil {
		return fmt.Errorf("part %d already uploaded", part)
	}
	block := make([]byte, len(contents))
	copy(block, contents)
	b.partial[part] = block
	return nil
}

func (b *BufferUploader) Close(final []byte) error {
	// we shouldn't need to acquire the lock here;
	// each Upload() call should have returned
	type part struct {
		id  int64
		mem []byte
	}
	lst := make([]part, 0, len(b.partial))
	var out []byte
	for k, v := range b.partial {
		lst = append(lst, part{
			id:  k,
			mem: v,
		})
	}
	sort.Slice(lst, func(i, j int) bool {
		return lst[i].id < lst[j].id
	})
	for i := range lst {
		out = append(out, lst[i].mem...)
	}
	out = append(out, final...)
	b.final = out
	return nil
}

// Bytes returns the final upload result
// after Close() has been called.
func (b *BufferUploader) Bytes() []byte { return b.final }

func (b *BufferUploader) Size() int64 { return int64(len(b.final)) }
