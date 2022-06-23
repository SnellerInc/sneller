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

package sorting

import (
	"fmt"
	"io"

	"github.com/SnellerInc/sneller/ion"
)

// RowsWriter creates Ion output from chunks of already encoded Ion structures.
// It gets input from sorting routine which yields an array of structures.
type RowsWriter struct {
	dst              io.Writer
	chunkAlignment   int
	written          int
	marshalledSymtab []byte
	scratch          []byte // extra buffer to build an Ion struct header
	chunk            []byte
}

func NewRowsWriter(dst io.Writer, symtab *ion.Symtab, chunkAlignment int) (*RowsWriter, error) {
	var buf ion.Buffer
	buf.StartChunk(symtab)
	marshalledSymtab := buf.Bytes()

	if len(marshalledSymtab) >= chunkAlignment {
		return nil, fmt.Errorf("chunk size %d too small, marshalled symbtab has %d",
			chunkAlignment, len(marshalledSymtab))
	}

	return &RowsWriter{
		dst:              dst,
		chunkAlignment:   chunkAlignment,
		written:          0,
		marshalledSymtab: marshalledSymtab,
		scratch:          make([]byte, 32),
		chunk:            make([]byte, chunkAlignment),
	}, nil
}

func (r *RowsWriter) write(bytes []byte) {
	r.written += copy(r.chunk[r.written:], bytes)
}

// WriteRows outputs the Ion records in the given order.
// It takes care on completing the Ion structure, but does
// not validate whether incoming data is sensible or not.
func (r *RowsWriter) WriteRows(records []IonRecord) (err error) {
	if r.written == 0 {
		r.writeHeader()
	}

	for _, record := range records {
		bytes := record.Bytes()
		size := ion.UnsafeWriteTag(r.scratch, ion.StructType, uint(len(bytes)))
		if r.written+size+len(bytes) > r.chunkAlignment {
			err := r.flush()
			if err != nil {
				return err
			}

			if len(r.marshalledSymtab)+size+len(bytes) > r.chunkAlignment {
				return fmt.Errorf("record cannot fit in a chunk - size of record: %d, size of symtab: %d, chunk alignment: %d",
					size+len(bytes), len(r.marshalledSymtab), r.chunkAlignment)
			} else {
				r.writeHeader()
			}
		}

		r.write(r.scratch[:size])
		r.write(bytes)
	}

	return nil
}

// WriteRecord outputs a single record
func (r *RowsWriter) WriteRecord(bytes []byte) (err error) {
	if r.written == 0 {
		r.writeHeader()
	}

	size := ion.UnsafeWriteTag(r.scratch, ion.StructType, uint(len(bytes)))
	if r.written+size+len(bytes) > r.chunkAlignment {
		err := r.flush()
		if err != nil {
			return err
		}

		if len(r.marshalledSymtab)+size+len(bytes) > r.chunkAlignment {
			return fmt.Errorf("record cannot fit in a chunk - size of record: %d, size of symtab: %d, chunk alignment: %d",
				size+len(bytes), len(r.marshalledSymtab), r.chunkAlignment)
		} else {
			r.writeHeader()
		}
	}

	r.write(r.scratch[:size])
	r.write(bytes)

	return nil
}

// Close implements io.Closer
func (r *RowsWriter) Close() error {
	return r.flush()
}

// writeHeader writes the head of IonChunk.
// It's the BVM marker followed by a symtab.
func (r *RowsWriter) writeHeader() error {
	r.write(r.marshalledSymtab)
	return nil
}

// flush flushes the current buffered row bytes
func (r *RowsWriter) flush() error {
	_, err := r.dst.Write(r.chunk[:r.written])
	if err != nil {
		return err
	}

	r.written = 0
	return nil
}
