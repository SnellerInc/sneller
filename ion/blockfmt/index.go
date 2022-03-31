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
	"crypto/rand"
	"crypto/subtle"
	"encoding/base32"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"

	"golang.org/x/crypto/blake2b"
)

// Version is the textual version
// of the objects produces by this
// package.
// If we start producing backwards-incompatible
// objects, this version number ought to be
// bumped.
const Version = "blockfmt/compressed/v2"

// IndexVersion is the current version
// number of the index format.
const IndexVersion = 1

// ErrIndexObsolete is returned when
// OpenIndex detects an index file with
// a version number lower than IndexVersion.
var ErrIndexObsolete = errors.New("index version obsolete")

// ObjectInfo is a collection of
// information about an object.
type ObjectInfo struct {
	// Path is the path to the
	// object. See fs.ValidPath
	// for a description of what constitutes
	// a valid path.
	//
	// ETag is the ETag of the object.
	// The ETag is opaque to the blockfmt
	// implementation.
	Path, ETag string
	// LastModified is the mtime of
	// the object. We use both the ETag
	// and the mtime to determine whether
	// an object has been modified since
	// we last looked at it.
	LastModified time.Time
	// Format specifies the format
	// of the object. For output
	// objects, the format indicates
	// the blockfmt version used to
	// write the ion object. For input
	// objects, the format describes the
	// conversion algorithm suffix used
	// to convert the object (see SuffixToFormat).
	Format string

	// Size, if non-zero, is the size of
	// the object. (Output objects are never 0 bytes.)
	Size int64
}

// Descriptor describes a single
// object within an Index.
type Descriptor struct {
	// ObjectInfo describes *this*
	// object's full path, ETag, and format.
	ObjectInfo
	// Trailer for the object. This may not always
	// be present in the index, in which case the
	// trailer must be read from the object.
	Trailer *Trailer

	// Original describes the original
	// input objects used to produce
	// this output object. This will be
	// nil if FlagSkipOrig was passed to
	// DecodeIndex.
	//
	// NOTE:
	// Original will only be present if
	// this object is small and therefore
	// a candidate for re-ingest. Otherwise
	// this field should not be present.
	Original []ObjectInfo
}

// Index is a collection of
// formatted objects with a name.
//
// Index objects are stored as MAC'd
// blobs in order to make it possible
// to detect tampering of the Contents
// of the index. (The modtime and ETag
// of the Contents are part of the signed
// payload, so we can refuse to operate
// on those objects if they fail to match
// the expected modtime and ETag.)
type Index struct {
	// Name is the name of the index.
	Name string
	// Created is the time the index
	// was populated.
	Created time.Time
	// Algo is the compression algorithm used to
	// compress the index contents.
	Algo string
	// Contents is the list of objects
	// that comprise the index.
	Contents []Descriptor
	// Inputs is the collection of
	// objects that comprise Contents.
	Inputs FileTree

	// LastScan is the time at which
	// the last scan operation completed.
	// This may be the zero time if no
	// scan has ever been performed.
	LastScan time.Time
	// Cursors is the list of scanning cursors.
	// These may not be present if no scan
	// has ever been performed.
	Cursors []string
	// Scanning indicates that scanning has
	// not yet completed.
	Scanning bool
}

const (
	// KeyLength is the length of
	// the key that needs to be provided
	// to Sign and DecodeIndex.
	// (The contents of the key should
	// be from a cryptographically secure
	// source of random bytes.)
	KeyLength = 32
	// SignatureLength is the length
	// of the signature appended
	// to the index objects.
	SignatureLength = KeyLength + 2

	rawSigLength = SignatureLength - 2
)

// Key is a shared secret key used
// to sign encoded Indexes.
type Key [KeyLength]byte

// appendSig appends a signature to 'data'
// using the provided key
func appendSig(key *Key, data []byte) ([]byte, error) {
	// prepend the signature with a nop pad
	// with the size of the signature,
	// so regular ion tooling will simply ignore
	// the appended signature
	data = append(data, 0x0e, 0x80|rawSigLength)
	h, err := blake2b.New256(key[:])
	if err != nil {
		return nil, err
	}
	h.Write(data)
	return h.Sum(data), nil
}

// Sign encodes an index in a binary format
// and signs it with the provided HMAC key.
//
// See DecodeIndex for authenticating and
// decoding a signed index blob.
func Sign(key *Key, idx *Index) ([]byte, error) {
	var buf ion.Buffer
	var st ion.Symtab
	var (
		version  = st.Intern("version")
		name     = st.Intern("name")
		created  = st.Intern("created")
		inputs   = st.Intern("inputs")
		isize    = st.Intern("input-size")
		scanning = st.Intern("scanning")
		cursors  = st.Intern("cursors")
		lastscan = st.Intern("last-scan")
	)
	var ibuf ion.Buffer
	buf.BeginStruct(-1)
	// begin with the version number
	// so that it's easier to write
	// a backwards-compatibility shim if we need it:
	buf.BeginField(version)
	buf.WriteInt(IndexVersion)
	buf.BeginField(name)
	buf.WriteString(idx.Name)
	buf.BeginField(created)
	buf.WriteTime(idx.Created)

	if !idx.LastScan.IsZero() {
		buf.BeginField(lastscan)
		buf.WriteTime(idx.LastScan)
	}
	if idx.Scanning {
		buf.BeginField(scanning)
		buf.WriteBool(true)
	}
	if len(idx.Cursors) > 0 {
		buf.BeginField(cursors)
		buf.BeginList(-1)
		for i := range idx.Cursors {
			buf.WriteString(idx.Cursors[i])
		}
		buf.EndList()
	}
	if len(idx.Contents) == 0 {
		// Do nothing...
	} else if idx.Algo != "" {
		idx.writeContents(&ibuf, &st)
		comp := Compression(idx.Algo)
		cbuf := comp.Compress(ibuf.Bytes(), malloc(ibuf.Size())[:0])
		buf.BeginField(st.Intern("algo"))
		buf.WriteString(idx.Algo)
		buf.BeginField(st.Intern("size"))
		buf.WriteInt(int64(ibuf.Size()))
		buf.BeginField(st.Intern("contents"))
		buf.WriteBlob(cbuf)
		free(cbuf)
	} else {
		buf.BeginField(st.Intern("contents"))
		idx.writeContents(&buf, &st)
	}

	// encode tree; choose to compress
	// when it would encode to more than 1kB
	{
		ibuf.Reset()
		idx.Inputs.encode(&ibuf, &st)
		size := int64(len(ibuf.Bytes()))
		if size < 1024 {
			buf.BeginField(inputs)
			buf.UnsafeAppend(ibuf.Bytes())
		} else {
			alg := idx.Algo
			if alg == "" {
				alg = "zstd"
			}
			comp := Compression(alg)
			buf.BeginField(isize)
			buf.WriteInt(int64(len(ibuf.Bytes())))
			cbuf := comp.Compress(ibuf.Bytes(), malloc(ibuf.Size())[:0])
			buf.BeginField(inputs)
			buf.WriteBlob(cbuf)
			free(cbuf)
		}
	}

	buf.EndStruct()
	tail := buf.Bytes()
	buf.Set(nil)
	st.Marshal(&buf, true)
	buf.UnsafeAppend(tail)
	return appendSig(key, buf.Bytes())
}

func (idx *Index) writeContents(buf *ion.Buffer, st *ion.Symtab) {
	var (
		path         = st.Intern("path")
		etag         = st.Intern("etag")
		lastModified = st.Intern("last-modified")
		format       = st.Intern("format")
		original     = st.Intern("original")
		trailer      = st.Intern("trailer")
		size         = st.Intern("size")
	)
	buf.BeginList(-1)
	for i := range idx.Contents {
		buf.BeginStruct(-1)
		buf.BeginField(path)
		buf.WriteString(idx.Contents[i].Path)
		buf.BeginField(etag)
		buf.WriteString(idx.Contents[i].ETag)
		if !idx.Contents[i].LastModified.IsZero() {
			buf.BeginField(lastModified)
			buf.WriteTime(idx.Contents[i].LastModified)
		}
		buf.BeginField(format)
		buf.WriteString(idx.Contents[i].Format)
		buf.BeginField(size)
		buf.WriteInt(idx.Contents[i].Size)
		if t := idx.Contents[i].Trailer; t != nil {
			buf.BeginField(trailer)
			t.Encode(buf, st)
		}
		buf.BeginField(original)
		lst := idx.Contents[i].Original
		buf.BeginList(-1)
		for j := range lst {
			buf.BeginStruct(-1)
			buf.BeginField(path)
			buf.WriteString(lst[j].Path)
			buf.BeginField(etag)
			buf.WriteString(lst[j].ETag)
			if !idx.Contents[i].LastModified.IsZero() {
				buf.BeginField(lastModified)
				buf.WriteTime(lst[j].LastModified)
			}
			buf.BeginField(format)
			buf.WriteString(lst[j].Format)
			buf.BeginField(size)
			buf.WriteInt(lst[j].Size)
			buf.EndStruct()
		}
		buf.EndList()
		buf.EndStruct()
	}
	buf.EndList()
}

var (
	// ErrBadMAC is returned when a signature
	// for an object does not match the
	// computed MAC.
	ErrBadMAC = errors.New("bad index signature")
)

func (o *ObjectInfo) set(field string, value []byte) ([]byte, bool, error) {
	var ret []byte
	var err error
	switch field {
	case "etag":
		o.ETag, ret, err = ion.ReadString(value)
	case "path":
		o.Path, ret, err = ion.ReadString(value)
	case "format":
		o.Format, ret, err = ion.ReadString(value)
	case "last-modified":
		o.LastModified, ret, err = ion.ReadTime(value)
	case "size":
		o.Size, ret, err = ion.ReadInt(value)
	default:
		return nil, false, nil
	}
	return ret, true, err
}

func (d *Descriptor) decode(td *TrailerDecoder, field []byte, opts Flag) error {
	return unpackStruct(td.Symbols, field, func(name string, field []byte) error {
		if name == "original" {
			if opts&FlagSkipOrig != 0 {
				return nil
			}
			return unpackList(field, func(item []byte) error {
				var o ObjectInfo
				err := unpackStruct(td.Symbols, item, func(name string, field []byte) error {
					_, ok, err := o.set(name, field)
					if !ok {
						return fmt.Errorf("unexpected field name %q", name)
					}
					return err
				})
				if err != nil {
					return fmt.Errorf("unpacking Original: %w", err)
				}
				d.Original = append(d.Original, o)
				return nil
			})
		}
		if name == "trailer" {
			t, err := td.Decode(field)
			if err != nil {
				return fmt.Errorf("unpacking Trailer: %w", err)
			}
			d.Trailer = t
			return nil
		}
		_, ok, err := d.set(name, field)
		if !ok {
			return fmt.Errorf("unexpected field %q", name)
		}
		return err
	})
}

// Flag is an option flag to be passed to DecodeIndex.
type Flag int

const (
	// FlagSkipOrig skips Index.Contents.Original
	// when decoding the index.
	FlagSkipOrig Flag = 1 << iota
)

func (idx *Index) readInputs(st *ion.Symtab, body []byte, isize int64, alg string) error {
	if ion.TypeOf(body) == ion.ListType {
		// stored decompressed
		return idx.Inputs.decode(st, body)
	}
	if alg == "" {
		alg = "zstd"
	}
	decomp := Decompression(alg)
	b, _, err := ion.ReadBytes(body)
	if err != nil {
		return fmt.Errorf("DecodeIndex: readInputs: %w", err)
	}
	contents := make([]byte, isize)
	if err := decomp.Decompress(b, contents); err != nil {
		return fmt.Errorf("DecodeIndex: readInputs: %w", err)
	}
	return idx.Inputs.decode(st, contents)
}

// DecodeIndex decodes a signed index (see Sign)
// and returns the Index, or an error if the index
// was malformed or the signature doesn't match.
//
// If FlagSkipOrig is passed in opts, this avoids
// decoding Index.Contents.Original.
//
// NOTE: the returned Index may contain fields
// that alias the input slice.
func DecodeIndex(key *Key, index []byte, opts Flag) (*Index, error) {
	if len(index) < SignatureLength {
		return nil, fmt.Errorf("encoded size %d too small to fit signature (%d)", len(index), SignatureLength)
	}
	h, err := blake2b.New256(key[:])
	if err != nil {
		return nil, err
	}
	// the two-byte pad is part of the signed payload,
	// so that's the point that marks the end of the
	// payload and the beginning of the signature
	split := len(index) - rawSigLength
	h.Write(index[:split])
	sum := h.Sum(nil)
	if subtle.ConstantTimeCompare(sum, index[split:]) != 1 {
		return nil, ErrBadMAC
	}
	// now decode the real thing
	var st ion.Symtab
	rest, err := st.Unmarshal(index[:split])
	if err != nil {
		return nil, err
	}
	idx := new(Index)
	td := TrailerDecoder{
		Symbols: &st,
	}
	var contents []byte
	var size, isize, version int64
	err = unpackStruct(&st, rest, func(name string, field []byte) (err error) {
		switch name {
		case "created":
			idx.Created, _, err = ion.ReadTime(field)
		case "name":
			idx.Name, _, err = ion.ReadString(field)
		case "contents":
			contents = field
		case "algo":
			idx.Algo, _, err = ion.ReadString(field)
		case "version":
			version, _, err = ion.ReadInt(field)
		case "size":
			size, _, err = ion.ReadInt(field)
		case "input-size":
			isize, _, err = ion.ReadInt(field)
		case "inputs":
			if opts&FlagSkipOrig == 0 {
				err = idx.readInputs(&st, field, isize, idx.Algo)
			}
		case "scanning":
			idx.Scanning, _, err = ion.ReadBool(field)
		case "cursors":
			err = unpackList(field, func(item []byte) error {
				str, _, err := ion.ReadString(item)
				if err != nil {
					return err
				}
				idx.Cursors = append(idx.Cursors, str)
				return nil
			})
		case "last-scan":
			idx.LastScan, _, err = ion.ReadTime(field)
		default:
			err = fmt.Errorf("unexpected field %q", name)
		}
		return
	})
	if err != nil {
		return nil, fmt.Errorf("DecodeIndex: decoding structure: %w", err)
	}
	// we don't currently maintain any backwards-compatibility shims:
	if version != IndexVersion {
		return nil, fmt.Errorf("%w %d", ErrIndexObsolete, version)
	}
	if contents == nil {
		return idx, nil
	}
	if ion.TypeOf(contents) == ion.BlobType {
		if idx.Algo == "" {
			return nil, fmt.Errorf("DecodeIndex: missing compression algorithm")
		}
		b, _, err := ion.ReadBytes(contents)
		if err != nil {
			return nil, fmt.Errorf("DecodeIndex: %w", err)
		}
		decomp := Decompression(idx.Algo)
		contents = malloc(int(size))
		defer free(contents)
		if err := decomp.Decompress(b, contents); err != nil {
			return nil, fmt.Errorf("DecodeIndex: %w", err)
		}
	}
	err = unpackList(contents, func(field []byte) error {
		var self Descriptor
		if err := self.decode(&td, field, opts); err != nil {
			return err
		}
		idx.Contents = append(idx.Contents, self)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("DecodeIndex: decoding Contents: %w", err)
	}
	return idx, nil
}

func uuid() string {
	var buf [16]byte
	_, err := rand.Read(buf[:])
	if err != nil {
		// crypto random source is busted?
		panic(err)
	}
	// remove the trailing padding; it is deterministic
	return strings.TrimSuffix(base32.StdEncoding.EncodeToString(buf[:]), "======")
}

// SyncInputs syncs idx.Inputs to a directory
// within idx.Inputs.Backing.
// Callers are required to call SyncInputs after
// updating idx.Inputs.
func (idx *Index) SyncInputs(dir string) error {
	return idx.Inputs.sync(func(buf []byte) (string, string, error) {
		p := path.Join(dir, "inputs-"+uuid())
		etag, err := idx.Inputs.Backing.WriteFile(p, buf)
		return p, etag, err
	})
}

// TimeRange returns the inclusive time range for the
// given path expression.
func (idx *Index) TimeRange(p *expr.Path) (min, max time.Time, ok bool) {
	for i := range idx.Contents {
		desc := &idx.Contents[i]
		if desc.Trailer == nil {
			continue
		}
		for i := range desc.Trailer.Blocks {
			blk := &desc.Trailer.Blocks[i]
			for _, r := range blk.Ranges {
				tr, _ := r.(*TimeRange)
				if tr == nil || !pathMatches(p, tr.path) {
					continue
				}
				if !ok {
					min, max, ok = tr.MinTime(), tr.MaxTime(), true
				} else {
					min, max = timeUnion(min, max, tr.MinTime(), tr.MaxTime())
				}
			}
		}
	}
	return min, max, ok
}

func pathMatches(e *expr.Path, p []string) bool {
	if len(p) == 0 || e.First != p[0] {
		return false
	}
	p = p[1:]
	for n := e.Rest; n != nil; n = n.Next() {
		if len(p) == 0 {
			return false
		}
		d, ok := n.(*expr.Dot)
		if !ok || d.Field != p[0] {
			return false
		}
		p = p[1:]
	}
	return len(p) == 0
}
