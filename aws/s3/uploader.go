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

package s3

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/SnellerInc/sneller/aws"
)

// Uploader wraps the state of a multi-part upload.
//
// To use an Uploader to create a multi-part object,
// populate all of the public fields of the Uploader
// and then call Uploader.Start, followed by zero or
// more calls to Uploader.UploadPart, followed by
// one call to Uploader.Close
type Uploader struct {
	// Key is the key used to sign requests.
	// It cannot be nil.
	Key *aws.SigningKey
	// Client is the http client used to
	// make requests. If it is nil, then
	// DefaultClient will be used.
	Client *http.Client

	// ContentType, if not an empty string,
	// will be the Content-Type of the new object.
	ContentType string

	Bucket, Object string

	Scheme string

	// Host, if not the empty string,
	// is the host of the bucket.
	// If Host is unset, then "s3.amazonaws.com" is used.
	//
	// Requests are always made to <bucket>.host
	Host string

	// Mbbs, if non-zero, is the expected
	// link speed in Mbps. This number is
	// used to determine the optimal parallelism
	// for uploads.
	// (For example, use Mbps = 25000 on a 25Gbps link, etc.)
	Mbps int

	// upload ID
	id string

	// next part
	part int64

	// ETag of the final result;
	// just the empty string until Close is called
	finalETag string

	// updated by Start and Close, respectively,
	// which require synchronization with concurrent
	// UploadPart calls
	started, finished bool

	// list of ETags collected as
	// parts are uploaded; these are
	// sent as part of the CompleteMultipartUpload call
	lock    sync.Mutex
	parts   []tagpart
	maxpart int64

	// background uploads
	bg       sync.WaitGroup
	asyncerr error
}

// MinPartSize returns the minimum part size
// for the Uploader.
//
// (The return value of MinPartSize is always s3.MinPartSize.)
func (u *Uploader) MinPartSize() int {
	return MinPartSize
}

type tagpart struct {
	Num  int64  `xml:"PartNumber"`
	ETag string `xml:"ETag"`
	size int64  `xml:"-"`
}

func (u *Uploader) req(method, uri, query string) *http.Request {
	obj := url.URL{
		Scheme:   u.Scheme,
		RawQuery: query,
	}
	if u.Key.BaseURI == "" {
		obj.Path = "/" + uri                      // fully decoded path
		obj.RawPath = "/" + almostPathEscape(uri) // escaped path
		obj.Host = u.Bucket + "." + u.Host
	} else {
		obj.Path = "/" + u.Bucket + "/" + uri                      // fully decoded path
		obj.RawPath = "/" + u.Bucket + "/" + almostPathEscape(uri) // escaped path
		obj.Host = u.Host

	}
	return &http.Request{
		Method: method,
		URL:    &obj,
		Header: make(http.Header),
	}
}

// Start begins a multipart upload.
// Start must be called exactly once,
// before any calls to WritePart are made.
func (u *Uploader) Start() error {
	if u.started {
		panic("multiple calls to Uploader.Start()")
	}
	if u.Key.BaseURI == "" {
		u.Scheme = "https"
		u.Host = "s3." + u.Key.Region + ".amazonaws.com"
	} else {
		uu, _ := url.Parse(u.Key.BaseURI)
		u.Scheme = uu.Scheme
		u.Host = uu.Host
	}
	if u.Client == nil {
		u.Client = &DefaultClient
	}
	if u.Bucket == "" || u.Object == "" {
		return fmt.Errorf("s3.Uploader.Bucket and s3.Uploader.Object must be present")
	}
	req := u.req("POST", u.Object, "uploads=")
	if u.ContentType != "" {
		req.Header.Set("Content-Type", u.ContentType)
	}
	u.Key.SignV4(req, nil)
	res, err := u.Client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("s3.Uploader.Start: %s %q", res.Status, extractMessage(res.Body))
	}
	rt := struct {
		Bucket string `xml:"Bucket"`
		Key    string `xml:"Key"`
		ID     string `xml:"UploadId"`
	}{}
	err = xml.NewDecoder(res.Body).Decode(&rt)
	if err != nil {
		return err
	}
	if rt.Bucket != u.Bucket {
		return fmt.Errorf("returned bucket %q not input bucket %q?", rt.Bucket, u.Bucket)
	}
	if rt.Key != u.Object {
		return fmt.Errorf("returned key %q not input key %q?", rt.Key, u.Object)
	}
	u.started = true
	u.id = rt.ID
	return nil
}

// NextPart atomically increments the internal
// part counter inside the uploader and returns
// the next available part number.
// Note that AWS multipart uploads have 1-based
// part numbers (i.e. the first part is part 1).
//
// If the data to be uploaded is intrinsically un-ordered,
// then NextPart() can be used to greedily assign part numbers.
//
// Note that currently the maximum part number
// allowed by AWS is 10000.
func (u *Uploader) NextPart() int64 {
	return atomic.AddInt64(&u.part, 1)
}

// MinPartSize is the minimum size for
// all of the parts of a multi-part upload
// except for the final part.
const MinPartSize = 5 * 1024 * 1024

// extractMessage tries to extract the <Message/>
// field of an XML response to improve error messages
func extractMessage(r io.Reader) string {
	rt := struct {
		Message string `xml:"Message"`
	}{}
	if xml.NewDecoder(r).Decode(&rt) == nil {
		return rt.Message
	}
	return "(no message)"
}

// Upload uploads the part number num from
// the ReadCloser r, which must return exactly size bytes of data.
// S3 prohibits multi-part upload parts smaller than 5MB (except
// for the final bytes), so size must be at least 5MB.
//
// It is safe to call Upload from multiple goroutines
// simultaneously. However, calls to Upload must be
// synchronized to occur strictly after a call to Start
// and strictly before a call to Close.
func (u *Uploader) Upload(num int64, contents []byte) error {
	if !u.started {
		panic("s3.Uploader.UploadPart before Start()")
	}
	if len(contents) < MinPartSize {
		return fmt.Errorf("UploadPart size %d below min part size %d", len(contents), MinPartSize)
	}
	return u.upload(num, contents)
}

func (u *Uploader) upload(num int64, contents []byte) error {
	req := u.req("PUT", u.Object, fmt.Sprintf("partNumber=%d&uploadId=%s", num, u.id))
	u.Key.SignV4(req, contents)
	res, err := flakyDo(u.Client, req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("UploadPart: %s %q", res.Status, extractMessage(res.Body))
	}
	etag := res.Header.Get("ETag")
	if etag == "" {
		return fmt.Errorf("s3.Uploader.UploadPart: response missing ETag?")
	}
	u.lock.Lock()
	if num > u.maxpart {
		u.maxpart = num
	}
	u.parts = append(u.parts, tagpart{
		Num:  num,
		ETag: etag,
		size: int64(len(contents)),
	})
	u.lock.Unlock()
	return nil
}

// CopyFrom performs a server side copy for the part number `num`.
//
// Set `start` and `end` to `0` to copy the entire source object.
//
// It is safe to call CopyFrom from multiple goroutines
// simultaneously. However, calls to CopyFrom must be
// synchronized to occur strictly after a call to Start
// and strictly before a call to Close.
//
// As an optimization, most of the work for CopyFrom is
// performed asynchronously. Callers must call Close and
// check its return value in order to correctly handle
// errors from CopyFrom.
func (u *Uploader) CopyFrom(num int64, source *Reader, start int64, end int64) error {
	if !u.started {
		panic("s3.Uploader.CopyFrom before Start()")
	}
	size := source.Size
	if start != 0 || end != 0 {
		if start < 0 || end < 0 {
			return errors.New("start and end values must be positive numbers")
		}
		if end > size {
			return fmt.Errorf("end value %d greater than source size %d", end, size)
		}
		size = end - start
	}
	if size < MinPartSize {
		return fmt.Errorf("CopyFrom size %d below min part size %d", size, MinPartSize)
	}

	// update the max part before launching anything
	// so that Close can perform an upload at the same time
	// as the copy-part operation is still happening
	u.lock.Lock()
	if num > u.maxpart {
		u.maxpart = num
	}
	u.lock.Unlock()

	u.bg.Add(1)
	go u.copy(num, source, start, end)
	return nil
}

func (u *Uploader) noteErr(err error) {
	u.lock.Lock()
	defer u.lock.Unlock()
	if u.asyncerr == nil {
		u.asyncerr = err
	}
}

func (u *Uploader) copy(num int64, source *Reader, start int64, end int64) {
	defer u.bg.Done()
	req := u.req("PUT", u.Object, fmt.Sprintf("partNumber=%d&uploadId=%s", num, u.id))
	req.Header.Add("x-amz-copy-source", fmt.Sprintf("/%s/%s", source.Bucket, source.Path))
	req.Header.Add("x-amz-copy-source-if-match", source.ETag)
	size := source.Size
	if start != 0 || end != 0 {
		size = end - start
		req.Header.Add("x-amz-copy-source-range", fmt.Sprintf("bytes=%d-%d", start, end-1))
	}
	u.Key.SignV4(req, nil)
	res, err := flakyDo(u.Client, req)
	if err != nil {
		u.noteErr(err)
		return
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		u.noteErr(fmt.Errorf("CopyFrom: %s %q", res.Status, extractMessage(res.Body)))
		return
	}
	var etag string
	rt := struct {
		ETag string `xml:"ETag"`
	}{}
	if xml.NewDecoder(res.Body).Decode(&rt) == nil {
		etag = rt.ETag
	}
	if etag == "" {
		u.noteErr(fmt.Errorf("s3.Uploader.CopyFrom: response missing ETag?"))
		return
	}
	u.lock.Lock()
	u.parts = append(u.parts, tagpart{
		Num:  num,
		ETag: etag,
		size: size,
	})
	u.lock.Unlock()
}

// CompletedParts returns the number of parts
// that have been successfully uploaded.
//
// It is safe to call CompletedParts from multiple
// goroutines that may also be calling UploadPart,
// but be wary of logical races involving the number
// of uploaded parts.
func (u *Uploader) CompletedParts() int {
	u.lock.Lock()
	defer u.lock.Unlock()
	return len(u.parts)
}

// Closed returns whether or not Close
// has been called on u.
func (u *Uploader) Closed() bool { return u.finished }

// ID returns the "Upload ID" of this upload.
// The return value of ID is only valid after
// Start has been called.
func (u *Uploader) ID() string { return u.id }

func (u *Uploader) Size() int64 {
	u.lock.Lock()
	defer u.lock.Unlock()
	if !u.finished {
		return 0
	}
	out := int64(0)
	for i := range u.parts {
		out += u.parts[i].size
	}
	return out
}

// Close uploads the final part of the multi-part upload
// and asks S3 to finalize the object from its constituent parts.
// (If size is zero, then r may be nil, in which case no final
// part is uploaded before the multi-part object is finalized.)
//
// Close will panic if Start has never been called
// or if Close has already been called and returned successfully.
func (u *Uploader) Close(final []byte) error {
	if !u.started {
		panic("s3.Uploader.Close before Start()")
	}
	if u.finished {
		panic("multiple calls to s3.Uploader.Close")
	}
	if len(final) > 0 {
		// it is safe to read maxpart here because
		// maxpart is updated in calls to CopyFrom and Upload,
		// and we've specified that it is not safe for the caller
		// to let those race with Close
		err := u.upload(u.maxpart+1, final)
		if err != nil {
			return err
		}
	}
	// wait for any/all CopyFrom operations to finish;
	// after this we know u.parts will be fully up-to-date
	u.bg.Wait()
	if u.asyncerr != nil {
		return u.asyncerr
	}

	// the S3 API barfs if parts are not in ascending order
	sort.Slice(u.parts, func(i, j int) bool {
		return u.parts[i].Num < u.parts[j].Num
	})

	req := u.req("POST", u.Object, fmt.Sprintf("uploadId=%s", u.id))
	req.Header.Set("Content-Type", "application/xml")
	buf, err := xml.Marshal(&struct {
		XMLName xml.Name  `xml:"CompleteMultipartUpload"`
		NS      string    `xml:"xmlns,attr"`
		Parts   []tagpart `xml:"Part"`
	}{
		NS:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Parts: u.parts,
	})
	if err != nil {
		return err
	}
	u.Key.SignV4(req, buf)

	res, err := flakyDo(u.Client, req)
	if err != nil {
		return fmt.Errorf("s3.Uploader.Close: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("s3.Uploader.Close: %s %q", res.Status, extractMessage(res.Body))
	}

	// This is a bit nasty:
	// the upload can fail after a 200 if we
	// get a response with <Error/>, so we have
	// to examine the xml name of the returned value
	// in order to determine if we got the write thing
	rt := struct {
		XMLName  xml.Name
		Location string `xml:"Location"`
		Bucket   string `xml:"Bucket"`
		Key      string `xml:"Key"`
		ETag     string `xml:"ETag"`

		// error fields:
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}{}
	err = xml.NewDecoder(res.Body).Decode(&rt)
	if err != nil {
		return fmt.Errorf("s3.Uploader.Close: decoding response: %w", err)
	}
	switch rt.XMLName.Local {
	default:
		return fmt.Errorf("s3.Uploader.Close: unexpected object %s", rt.XMLName.Local)
	case "Error":
		return fmt.Errorf("s3.Uploader.Close: %s %s", rt.Code, rt.Message)
	case "CompleteMultipartUploadResult":
		// ok; this is what we want
	}
	u.finalETag = rt.ETag
	u.finished = true
	return nil
}

// ETag returns the ETag of the final upload.
// The return value of ETag is only valid after
// Close has been called.
func (u *Uploader) ETag() string {
	return u.finalETag
}

func (u *Uploader) idealParallel(parts int64) int {
	const max = 40
	res := max
	if u.Mbps != 0 {
		// guess 640Mbps = 80MB/s per connection
		// (S3 guidelines say 85-90MB/s)
		res = u.Mbps / 800
	}
	if parts < int64(res) && parts > 0 {
		return int(parts)
	}
	if res <= 0 {
		return 1
	}
	return res
}

// Abort aborts a multi-part upload.
//
// Abort is *not* safe to call concurrently
// with Start, Close, or UploadPart.
//
// If Start has not been called on the Uploader,
// or if the uploader has successfully finished
// uploading, Abort does nothing.
//
// If Abort is called on a partially-finished Upload
// and returns without an error, then the state of
// the Uploader is reset so that Start may be called
// again to re-try the upload.
func (u *Uploader) Abort() error {
	if !u.started || u.finished {
		return nil
	}
	u.bg.Wait()
	req := u.req("DELETE", u.Object, fmt.Sprintf("uploadId=%s", u.id))
	u.Key.SignV4(req, nil)

	res, err := u.Client.Do(req)
	if err != nil {
		return fmt.Errorf("s3.Uploader.Abort: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 204 {
		return fmt.Errorf("s3.Uploader.Abort: %s %s", res.Status, extractMessage(res.Body))
	}

	// reset internal state
	u.part = 0
	u.started = false
	u.finished = false
	u.id = ""
	u.parts = nil
	return nil
}

// UploadReaderAt is a utility method that performs
// a parallel upload of an io.ReaderAt of a given size.
//
// UploadReaderAt closes the Uploader after uploading
// the entirety of the contents of r.
//
// UploadReaderAt is not safe to call concurrently with
// UploadPart or Close.
func (u *Uploader) UploadReaderAt(r io.ReaderAt, size int64) error {
	const partSize = 8 * 1024 * 1024
	nonfinal := size / partSize
	endparts := nonfinal * partSize
	offset := int64(0)
	parallel := u.idealParallel(nonfinal)
	var wg sync.WaitGroup
	wg.Add(parallel)
	errlist := make([]error, parallel)
	for i := 0; i < parallel; i++ {
		go func(i int) {
			defer wg.Done()
			buf := make([]byte, partSize)
			for {
				loff := atomic.AddInt64(&offset, partSize) - partSize
				if loff >= endparts {
					break
				}
				// 1-based part numbers
				part := (loff / partSize) + 1
				n, err := r.ReadAt(buf, loff)
				if n < partSize {
					if err == nil || errors.Is(err, io.EOF) {
						err = io.ErrUnexpectedEOF
					}
					errlist[i] = err
					return
				}
				err = u.Upload(part, buf)
				if err != nil {
					errlist[i] = fmt.Errorf("s3.UploadReaderAt part %d: %w", part, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	for i := range errlist {
		if errlist[i] != nil {
			return errlist[i]
		}
	}
	var tail []byte
	tailsize := int(size - endparts)
	if tailsize > 0 {
		tail = make([]byte, tailsize)
		n, err := r.ReadAt(tail, endparts)
		if n < tailsize {
			if err == nil || errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
	}
	return u.Close(tail)
}
