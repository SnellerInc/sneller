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
	"io/fs"
	"path"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/SnellerInc/sneller/compr"
	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

// IndirectTree is an ordered list of IndirectRefs.
//
// See IndirectTree.Append for adding descriptors.
type IndirectTree struct {
	// Refs is the list of objects containing
	// lists of descriptors, from oldest to newest.
	Refs []IndirectRef
}

// IndirectRef references an object
// that contains a list of descriptors.
type IndirectRef struct {
	ObjectInfo
	// Objects is the number of
	// object references inside
	// the packed file pointed to by Path.
	Objects int
	// Ranges is the summary of all ranges
	// stored in the objects pointed to by Path.
	Ranges []Range
}

// Objects returns the total number
// of objects referenced by the indirect tree.
func (i *IndirectTree) Objects() int {
	n := 0
	for j := range i.Refs {
		n += i.Refs[j].Objects
	}
	return n
}

func (i *IndirectTree) encode(st *ion.Symtab, buf *ion.Buffer) {
	path := st.Intern("path")
	etag := st.Intern("etag")
	lastModified := st.Intern("last-modified")
	size := st.Intern("size")
	objects := st.Intern("objects")
	ranges := st.Intern("ranges")

	buf.BeginStruct(-1)
	buf.BeginField(st.Intern("refs"))
	buf.BeginList(-1)
	for j := range i.Refs {
		buf.BeginStruct(-1)
		buf.BeginField(path)
		buf.WriteString(i.Refs[j].Path)
		buf.BeginField(etag)
		buf.WriteString(i.Refs[j].ETag)
		buf.BeginField(lastModified)
		buf.WriteTime(i.Refs[j].LastModified)
		buf.BeginField(size)
		buf.WriteInt(i.Refs[j].Size)
		buf.BeginField(objects)
		buf.WriteInt(int64(i.Refs[j].Objects))
		buf.BeginField(ranges)
		writeRanges(buf, st, i.Refs[j].Ranges)
		buf.EndStruct()
	}
	buf.EndList()
	buf.EndStruct()
}

func (i *IndirectTree) parse(td *TrailerDecoder, body []byte) error {
	return unpackStruct(td.Symbols, body, func(name string, field []byte) error {
		switch name {
		case "refs":
			return unpackList(field, func(field []byte) error {
				var ir IndirectRef
				err := unpackStruct(td.Symbols, field, func(name string, field []byte) error {
					switch name {
					case "ranges":
						ranges, err := td.unpackRanges(td.Symbols, field)
						if err != nil {
							return err
						}
						ir.Ranges = ranges
						return nil
					case "objects":
						n, _, err := ion.ReadInt(field)
						if err != nil {
							return err
						}
						ir.Objects = int(n)
						return nil
					default:
						_, _, err := ir.ObjectInfo.set(name, field)
						return err
					}
				})
				if err != nil {
					return err
				}
				i.Refs = append(i.Refs, ir)
				return nil
			})
		default:
			return fmt.Errorf("IndirectTree.parse: unexpected field name %q", name)
		}
	})
}

func keepAny(t *Trailer, keep func([]Range) bool) bool {
	for i := range t.Blocks {
		if keep(t.Blocks[i].Ranges) {
			return true
		}
	}
	return false
}

func (i *IndirectTree) decode(ifs InputFS, src *IndirectRef, in []Descriptor, keep func([]Range) bool) ([]Descriptor, error) {
	f, err := ifs.Open(src.Path)
	if err != nil {
		return in, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return in, err
	}
	etag, err := ifs.ETag(src.Path, info)
	if err != nil {
		return in, err
	}
	if etag != src.ETag {
		return in, fmt.Errorf("in IndirectTree: ETag changed: %s -> %s", src.ETag, etag)
	}
	// the contents of the object
	// pointed to by an IndirectRef
	// is a zstd-compressed bytestream
	// wrapped in an ion 'blob' header;
	// the contents of the decompressed
	// bytestream is
	//   {'contents': [descriptors...]}
	// (with a leading symbol table)
	buf := make([]byte, info.Size())
	_, err = io.ReadFull(f, buf)
	if err != nil {
		return in, fmt.Errorf("IndirectTree: io.ReadFull: %w", err)
	}
	buf, err = compr.DecodeZstd(buf, nil)
	var st ion.Symtab
	buf, err = st.Unmarshal(buf)
	if err != nil {
		return in, fmt.Errorf("IndirectTree.load: %w", err)
	}
	var td TrailerDecoder
	td.Symbols = &st
	err = unpackStruct(&st, buf, func(name string, field []byte) error {
		switch name {
		case "contents":
			return unpackList(field, func(field []byte) error {
				var d Descriptor
				err := d.decode(&td, field, 0)
				if err != nil {
					return err
				}
				if keep == nil || keepAny(d.Trailer, keep) {
					in = append(in, d)
				}
				return nil
			})
		default:
			return fmt.Errorf("unrecognized field %q", name)
		}
		return nil
	})
	return in, err
}

// Search traverses the IndirectTree through
// the backing store (ifs) to produce the
// list of blobs that match the given predicate.
func (i *IndirectTree) Search(ifs InputFS, overlap func(r []Range) bool) ([]Descriptor, error) {
	var descs []Descriptor
	var err error
	for j := range i.Refs {
		if overlap != nil && !overlap(i.Refs[j].Ranges) {
			continue
		}
		descs, err = i.decode(ifs, &i.Refs[j], descs, overlap)
		if err != nil {
			return descs, err
		}
	}
	return descs, nil
}

// targetRefSize is the target size of stored refs;
// we keep appending to an IndirectRef until its
// compressed size exceeds this threshold
//
// (the number of descriptors that fit in this range
// will depend on the compression ratio and the number
// of sparse indices, but "a few hundred bytes" is a good
// approximation of the compressed size of one ref)
var targetRefSize = 256 * 1024

// append appends a list of descriptors to the tree
// and writes any new decriptor lists to files in basedir
// relative to the root of ofs.
func (idx *Index) append(i *IndirectTree, ofs UploadFS, basedir string, lst []Descriptor, expiry time.Duration) error {
	if len(lst) == 0 {
		return nil
	}
	var base []*TimeRange
	var prepend []Descriptor
	var err error
	var r *IndirectRef

	var prev string
	if len(i.Refs) > 0 && i.Refs[len(i.Refs)-1].Size < int64(targetRefSize) {
		r = &i.Refs[len(i.Refs)-1]
		prev = r.Path
		base = toTimeRanges(r.Ranges)
		prepend, err = i.decode(ofs, r, nil, nil)
		if err != nil {
			return err
		}
	} else {
		i.Refs = append(i.Refs, IndirectRef{})
		r = &i.Refs[len(i.Refs)-1]
	}

	for i := range lst {
		for j := range lst[i].Trailer.Blocks {
			blk := &lst[i].Trailer.Blocks[j]
			if len(blk.Ranges) == 0 {
				continue
			}
			if base == nil {
				base = copyTimeRanges(toTimeRanges(blk.Ranges))
			} else {
				base = union(base, toTimeRanges(blk.Ranges))
			}
		}
	}

	all := append(prepend, lst...)

	// encode the list of objects:
	var buf ion.Buffer
	var st ion.Symtab
	buf.BeginStruct(-1)
	buf.BeginField(st.Intern("contents"))
	writeContents(&buf, &st, all)
	buf.EndStruct()

	split := buf.Size()
	st.Marshal(&buf, true)
	contents := buf.Bytes()
	symtab, body := contents[split:], contents[:split]
	e, _ := zstd.NewWriter(nil)
	compressed := e.EncodeAll(append(symtab, body...), nil)

	p := path.Join(basedir, "indirect-"+uuid())
	etag, err := ofs.WriteFile(p, compressed)
	if err != nil {
		return err
	}
	r.Path = p
	r.ETag = etag
	r.Size = int64(len(compressed))
	r.Objects = len(all)
	r.Ranges = toRanges(base)

	info, err := fs.Stat(ofs, p)
	if err != nil {
		return err
	}
	storedEtag, err := ofs.ETag(p, info)
	if err != nil {
		return err
	}
	if storedEtag != etag {
		return fmt.Errorf("stored etag is %s instead of %s?", storedEtag, etag)
	}
	r.LastModified = date.FromTime(info.ModTime())
	if prev != "" {
		idx.ToDelete = append(idx.ToDelete, Quarantined{
			Path:   prev,
			Expiry: date.Now().Add(expiry),
		})
	}
	return nil
}
