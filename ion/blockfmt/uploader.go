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
			// GCS has an S3 interoperability layer, but it doesn't
			// support the `x-amz-copy-source-range` header. See also
			// https://cloud.google.com/storage/docs/migrating#custommeta
			if up.Host != "storage.googleapis.com" {
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
	}
	var buffer []byte
	target := dst.MinPartSize()
	if target == 1 {
		// this is a BufferUploader
		target = 64 * 1024
	}
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
