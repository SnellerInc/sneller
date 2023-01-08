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
	"net/url"
	"time"
	"unsafe"

	"github.com/SnellerInc/sneller/compr"
	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// If the size in bytes of an encoded list exceeds this
// threshold, the blob will be written compressed.
const compressionThreshold = 16 * 1024

// compressor will be used to encode large compressed
// blob lists.
var compressor = compr.Compression("s2")

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
	// Ephemeral, if set, indicates that this
	// blob should be prioritized as a candidate
	// for eviction from a cache.
	Ephemeral bool
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

func (u *URL) encode(be *blobEncoder, dst *ion.Buffer, st *ion.Symtab) {
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
	if u.Info.Ephemeral {
		dst.BeginField(st.Intern("ephemeral"))
		dst.WriteBool(u.Info.Ephemeral)
	}
	if u.UnsafeNoIfMatch {
		dst.BeginField(st.Intern("no-if-match"))
		dst.WriteBool(true)
	}
	dst.EndStruct()
}

type decodeURL struct {
	parent *blobDecoder
	url    *URL
}

func (d *decodeURL) getInterface() Interface {
	return d.url
}

func (d *decodeURL) Init(*ion.Symtab) {
	d.url = d.parent.url()
}

func (d *decodeURL) SetField(name string, body []byte) error {
	var err error
	var b []byte
	switch name {
	case "value":
		b, _, err = ion.ReadStringShared(body)
		d.url.Value = d.parent.string(b)
	case "etag":
		b, _, err = ion.ReadStringShared(body)
		d.url.Info.ETag = d.parent.string(b)
	case "size":
		d.url.Info.Size, _, err = ion.ReadInt(body)
	case "align":
		var align int64
		align, _, err = ion.ReadInt(body)
		d.url.Info.Align = int(align)
	case "last-modified":
		d.url.Info.LastModified, _, err = ion.ReadTime(body)
	case "ephemeral":
		d.url.Info.Ephemeral, _, err = ion.ReadBool(body)
	case "no-if-match":
		d.url.UnsafeNoIfMatch, _, err = ion.ReadBool(body)
	default:
		return fmt.Errorf("unrecognized field")
	}

	return err
}

func (d *decodeURL) Finalize() error {
	return nil
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

func (u *URL) req(start, size int64) (*http.Request, error) {
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
	return req, nil
}

func redactQuery(err error) error {
	ue, ok := err.(*url.Error)
	if !ok {
		return err
	}
	u, _ := url.Parse(ue.URL)
	if u == nil {
		return err
	}
	u.RawQuery = ""
	u.RawFragment = ""
	ue.URL = u.String()
	return ue
}

func flakyGet(c *http.Client, req *http.Request) (*http.Response, error) {
	res, err := c.Do(req)
	if req.Body != nil ||
		(err == nil && res.StatusCode != 500 && res.StatusCode != 503) {
		return res, redactQuery(err)
	}
	// force re-dialing, which will hopefully
	// lead to a load balancer picking a healthy backend...?
	c.CloseIdleConnections()
	res, err = c.Do(req)
	return res, redactQuery(err)
}

// Reader implements blob.Interface.Reader
func (u *URL) Reader(start, size int64) (io.ReadCloser, error) {
	req, err := u.req(start, size)
	if err != nil {
		return nil, err
	}
	res, err := flakyGet(u.client(), req)
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
	var tmp ion.Buffer
	l.encode(&tmp, st)
	size := tmp.Size()
	if size <= compressionThreshold {
		dst.UnsafeAppend(tmp.Bytes())
		return
	}
	// rewind and write compressed
	data := compressor.Compress(tmp.Bytes(), nil)
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("algo"))
	dst.WriteString(compressor.Name())
	dst.BeginField(st.Intern("size"))
	dst.WriteInt(int64(size))
	dst.BeginField(st.Intern("data"))
	dst.WriteBlob(data)
	dst.EndStruct()
}

func (l *List) encode(dst *ion.Buffer, st *ion.Symtab) {
	var be blobEncoder
	dst.BeginList(-1)
	for i := range l.Contents {
		be.encode(l.Contents[i], dst, st)
	}
	dst.EndList()
}

func DecodeList(st *ion.Symtab, body []byte) (*List, error) {
	if ion.TypeOf(body) != ion.StructType {
		return decodeList(st, body)
	}
	var dec compr.Decompressor
	var buf []byte
	_, err := ion.UnpackStruct(st, body, func(name string, inner []byte) error {
		switch name {
		case "algo":
			algo, _, err := ion.ReadStringShared(inner)
			if err != nil {
				return err
			}
			dec = compr.Decompression(string(algo))
		case "size":
			size, _, err := ion.ReadInt(inner)
			if err != nil {
				return err
			}
			buf = make([]byte, size)
		case "data":
			if dec == nil || buf == nil {
				return fmt.Errorf("blob.DecodeList: missing algo or size")
			}
			data, _, err := ion.ReadBytesShared(inner)
			if err != nil {
				return err
			}
			return dec.Decompress(data, buf)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return decodeList(st, buf)
}

func decodeList(st *ion.Symtab, body []byte) (*List, error) {
	l := &List{}
	d := blobDecoder{}
	d.td.Symbols = st

	_, err := ion.UnpackList(body, func(buf []byte) error {
		inner, _, err := d.decode(buf)
		if err != nil {
			return err
		}

		l.Contents = append(l.Contents, inner)
		return nil
	})

	return l, err
}

type blobEncoder struct {
	nextID   int
	interned map[*Compressed]int
}

func (b *blobEncoder) id(c *Compressed) (int, bool) {
	if b.interned == nil {
		return 0, false
	}
	id, ok := b.interned[c]
	return id, ok
}

func (b *blobEncoder) intern(c *Compressed) {
	if b.interned == nil {
		b.interned = make(map[*Compressed]int)
	}
	// always start at 1 just to be certain
	b.nextID++
	b.interned[c] = b.nextID
}

func (b *blobEncoder) encode(i Interface, dst *ion.Buffer, st *ion.Symtab) {
	type encoder interface {
		encode(b *blobEncoder, dst *ion.Buffer, st *ion.Symtab)
	}
	if e, ok := i.(encoder); ok {
		e.encode(b, dst, st)
		return
	}
	dst.WriteString(fmt.Sprintf("cannot encode %T", i))
}

type blobDecoder struct {
	interned []*Compressed
	urls     []URL
	comps    []Compressed
	strcap   int
	str      []byte
	td       blockfmt.TrailerDecoder
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

type interfaceDecoder interface {
	ion.StructParser
	getInterface() Interface
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

	var dec interfaceDecoder

	settype := func(typename string) error {
		switch typename {
		case "blob.URL":
			dec = &decodeURL{parent: d}
		case "blob.Compressed":
			dec = &decodeComp{parent: d}
		case "blob.CompressedPart":
			dec = &decodeCPart{parent: d}
		default:
			return fmt.Errorf("unrecognized blob type %q", typename)
		}

		dec.Init(st)
		return nil
	}

	setitem := func(name string, body []byte) error {
		return dec.SetField(name, body)
	}

	rest, err := ion.UnpackTypedStruct(st, buf, settype, setitem)
	var err2 error
	if dec != nil {
		err2 = dec.Finalize()
	}
	if err == nil {
		err = err2
	}
	if err != nil {
		return nil, nil, fmt.Errorf("blob.DecodeList: %w", err)
	}

	return dec.getInterface(), rest, nil
}
