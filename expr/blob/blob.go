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

package blob

import (
	"fmt"
	"io"
	"net/http"
	"time"
	"unsafe"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// Interface is the interface
// implemented by every blob type.
type Interface interface {
	// Stat should return the Info
	// associated with this blob.
	Stat() (*Info, error)

	// Reader should return an io.ReadCloser
	// that reads the contents of the blob
	// from start to start+size bytes.
	//
	// Blobs are *required* to implement
	// this for the range (0, Info.Size),
	// but they are encouraged to implement
	// it for arbitrary byte ranges.
	Reader(start, size int64) (io.ReadCloser, error)
}

// Info is a uniform set of
// metadata about a blob.
type Info struct {
	// ETag is an opaque entity tag
	// that uniquely identifies a blob.
	// No two blobs will share an ETag
	// unless the contents of the blobs
	// is identical.
	ETag string
	// Size is the size of the blob in bytes.
	Size int64
	// Align is the alignment of chunks within the blob.
	Align int
	// LastModified is the last modified time
	// of the blob.
	LastModified date.Time
}

// Use sets the http client used to
// fetch the blob's contents.
func Use(i Interface, client *http.Client) {
	if u, ok := i.(*URL); ok {
		u.Client = client
		return
	}
	if c, ok := i.(*Compressed); ok {
		Use(c.From, client)
		return
	}
}

// URL is a blob that is fetched
// using ranged reads of an HTTP(S) URL
type URL struct {
	// Value is the base URL from which
	// data will be fetched.
	Value string
	// Info is the info associated with the blob.
	// ReadAt will take care to ensure that the
	// Last-Modified and ETag headers in HTTP responses
	// match the provided LastModified and ETag fields.
	// ReadAt also sends an If-Match precondition
	// in the requests unless UnsafeNoIfMatch is set.
	Info Info

	// UnsafeNoIfMatch, if set, will
	// cause HTTP GETs to avoid setting
	// the If-Match header to Info.ETag.
	// You should only unset this in testing.
	UnsafeNoIfMatch bool

	// Client, if non-nil, will
	// be used for HTTP fetches
	// in URL.Reader
	Client *http.Client
}

func (u *URL) client() *http.Client {
	if u.Client == nil {
		return http.DefaultClient
	}
	return u.Client
}

func (u *URL) encode(dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("type"))
	dst.WriteSymbol(st.Intern("blob.URL"))
	dst.BeginField(st.Intern("value"))
	dst.WriteString(u.Value)
	dst.BeginField(st.Intern("etag"))
	dst.WriteString(u.Info.ETag)
	dst.BeginField(st.Intern("size"))
	dst.WriteInt(u.Info.Size)
	dst.BeginField(st.Intern("align"))
	dst.WriteInt(int64(u.Info.Align))
	if !u.Info.LastModified.IsZero() {
		dst.BeginField(st.Intern("last-modified"))
		dst.WriteTime(u.Info.LastModified)
	}
	if u.UnsafeNoIfMatch {
		dst.BeginField(st.Intern("no-if-match"))
		dst.WriteBool(true)
	}
	dst.EndStruct()
}

func (d *blobDecoder) decodeURL(fields []byte) (*URL, error) {
	u := d.url()
	st := d.td.Symbols
	var b []byte
	var err error
	var sym ion.Symbol
	for len(fields) > 0 {
		sym, fields, err = ion.ReadLabel(fields)
		if err != nil {
			return nil, err
		}
		switch st.Get(sym) {
		case "value":
			b, fields, err = ion.ReadStringShared(fields)
			u.Value = d.string(b)
		case "etag":
			b, fields, err = ion.ReadStringShared(fields)
			u.Info.ETag = d.string(b)
		case "size":
			u.Info.Size, fields, err = ion.ReadInt(fields)
		case "align":
			var align int64
			align, fields, err = ion.ReadInt(fields)
			u.Info.Align = int(align)
		case "last-modified":
			u.Info.LastModified, fields, err = ion.ReadTime(fields)
		case "no-if-match":
			u.UnsafeNoIfMatch, fields, err = ion.ReadBool(fields)
		default:
			err = fmt.Errorf("unrecognized field %q", st.Get(sym))
		}
		if err != nil {
			return nil, fmt.Errorf("blob.URL decode: %w", err)
		}
	}
	return u, nil
}

// Stat implements blob.Interface.Stat
func (u *URL) Stat() (*Info, error) {
	out := new(Info)
	*out = u.Info
	return out, nil
}

func (u *URL) String() string {
	return u.Value
}

// Reader implements blob.Interface.Reader
func (u *URL) Reader(start, size int64) (io.ReadCloser, error) {
	req, err := http.NewRequest(http.MethodGet, u.Value, nil)
	if err != nil {
		return nil, err
	}
	end := start + size
	if end > u.Info.Size {
		end = u.Info.Size
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end-1))
	// ensure that we are still requesting
	// the same object that is specified by
	// the original ETag
	if !u.UnsafeNoIfMatch {
		req.Header.Set("If-Match", u.Info.ETag)
	}
	res, err := u.client().Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusPartialContent {
		res.Body.Close()
		return nil, fmt.Errorf("unexpected HTTP response status %d", res.StatusCode)
	}

	// if we got an ETag back, let's check it
	et := res.Header.Get("ETag")
	if et != "" && u.Info.ETag != "" && et != u.Info.ETag {
		res.Body.Close()
		return nil, fmt.Errorf("unexpected ETag in response %q", et)
	}
	// NOTE: we're doing this here because when
	// you send both If-Match and If-Unmodified-Since to S3,
	// then S3 prefers If-Match, so we can't enforce both up front
	lm := res.Header.Get("Last-Modified")
	if lm != "" && !u.Info.LastModified.IsZero() {
		t, err := time.Parse(time.RFC1123, lm)
		if err != nil {
			res.Body.Close()
			return nil, fmt.Errorf("parsing Last-Modified: %s", err)
		}
		// FIXME: re-enable this check;
		// See issue #790
		if false && t.After(u.Info.LastModified.Time()) {
			res.Body.Close()
			return nil, fmt.Errorf("Last-Modified time %s after blob.URL.LastModified %s", lm, u.Info.LastModified)
		}
	}
	return res.Body, nil
}

// List implements expr.Opaque
type List struct {
	Contents []Interface
}

func (l *List) String() string {
	return fmt.Sprintf("blobs%v", l.Contents)
}

func (l *List) TypeName() string { return "blob.List" }

func (l *List) Encode(dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginList(-1)
	for i := range l.Contents {
		encode(l.Contents[i], dst, st)
	}
	dst.EndList()
}

func DecodeList(st *ion.Symtab, body []byte) (*List, error) {
	if ion.TypeOf(body) != ion.ListType {
		return nil, fmt.Errorf("blob.DecodeList: unexpected ion type %v", ion.TypeOf(body))
	}
	l := &List{}
	var inner Interface
	var err error
	d := blobDecoder{}
	d.td.Symbols = st
	body, _ = ion.Contents(body)
	for len(body) > 0 {
		inner, body, err = d.decode(body)
		if err != nil {
			return nil, err
		}
		l.Contents = append(l.Contents, inner)
	}
	return l, nil
}

func encode(i Interface, dst *ion.Buffer, st *ion.Symtab) {
	type encoder interface {
		encode(dst *ion.Buffer, st *ion.Symtab)
	}
	if e, ok := i.(encoder); ok {
		e.encode(dst, st)
		return
	}
	dst.WriteString(fmt.Sprintf("cannot encode %T", i))
}

type blobDecoder struct {
	urls   []URL
	comps  []Compressed
	strcap int
	str    []byte
	td     blockfmt.TrailerDecoder
}

func (d *blobDecoder) url() *URL {
	if len(d.urls) == cap(d.urls) {
		d.urls = make([]URL, 0, 8+2*cap(d.urls))
	}
	d.urls = d.urls[:len(d.urls)+1]
	return &d.urls[len(d.urls)-1]
}

func (d *blobDecoder) compressed() *Compressed {
	if len(d.comps) == cap(d.comps) {
		d.comps = make([]Compressed, 0, 8+2*cap(d.comps))
	}
	d.comps = d.comps[:len(d.comps)+1]
	return &d.comps[len(d.comps)-1]
}

// string copies b to a contiguous buffer and returns a
// string aliasing it.
func (d *blobDecoder) string(b []byte) string {
	if len(b) > cap(d.str) {
		d.strcap = len(b) + 2*d.strcap
		d.str = make([]byte, 0, d.strcap)
	}
	d.str = d.str[:len(b)]
	copy(d.str, b)
	s := *(*string)(unsafe.Pointer(&d.str))
	d.str = d.str[len(d.str):]
	return s
}

func (d *blobDecoder) decode(buf []byte) (Interface, []byte, error) {
	st := d.td.Symbols
	if ion.TypeOf(buf) != ion.StructType {
		if ion.TypeOf(buf) == ion.StringType {
			str, _, err := ion.ReadString(buf)
			if str != "" && err == nil {
				return nil, nil, fmt.Errorf("blob.DecodeList: %s", str)
			}
		}
		return nil, nil, fmt.Errorf("blob.DecodeList: unexpected blob ion type %v", ion.TypeOf(buf))
	}
	typesym, _ := st.Symbolize("type")
	fields, rest := ion.Contents(buf)
	var sym ion.Symbol
	var err error
	for len(fields) > 0 {
		sym, fields, err = ion.ReadLabel(fields)
		if err != nil {
			return nil, nil, err
		}
		if sym == typesym {
			sym, fields, err = ion.ReadSymbol(fields)
			if err != nil {
				return nil, nil, err
			}
			switch st.Get(sym) {
			case "blob.URL":
				u, err := d.decodeURL(fields)
				return u, rest, err
			case "blob.Compressed":
				c, err := d.decodeComp(fields)
				return c, rest, err
			default:
				return nil, nil, fmt.Errorf("unrecognized blob type %q", st.Get(sym))
			}
		}
		// skip to next field+value
		fields = fields[ion.SizeOf(fields):]
	}
	return nil, nil, fmt.Errorf("blob.DecodeList: missing 'type' field")
}
