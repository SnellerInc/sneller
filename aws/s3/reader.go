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

// Package s3 implements a lightweight
// client of the AWS S3 API.
//
// The Reader type can be used to view
// S3 objects as an io.Reader or io.ReaderAt.
package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/SnellerInc/sneller/aws"
)

// DefaultClient is the default HTTP client
// used for requests made from this package.
var DefaultClient = http.Client{
	Transport: &http.Transport{
		ResponseHeaderTimeout: 60 * time.Second,
		// Empirically, AWS creates about 40
		// DNS entries for S3, so 5 connections
		// per host is about 100 total connections.
		// (Note that the default here is 2!)
		MaxIdleConnsPerHost: 5,
		// Don't set Accept-Encoding: gzip
		// because it leads to the go client natively
		// decompressing gzipped objects.
		DisableCompression: true,
		// AWS S3 occasionally provides "dead" hosts
		// in their round-robin DNS responses, and the
		// fastest way to identify them is during
		// connection establishment:
		DialContext: (&net.Dialer{
			Timeout: 2 * time.Second,
		}).DialContext,
	},
}

var (
	// ErrInvalidBucket is returned from calls that attempt
	// to use a bucket name that isn't valid according to
	// the S3 specification.
	ErrInvalidBucket = errors.New("invalid bucket name")
	// ErrETagChanged is returned from read operations where
	// the ETag of the underlying file has changed since
	// the file handle was constructed. (This package guarantees
	// that file read operations are always consistent with respect
	// to the ETag originally associated with the file handle.)
	ErrETagChanged = errors.New("file ETag changed")
)

func badBucket(name string) error {
	return fmt.Errorf("%w: %s", ErrInvalidBucket, name)
}

// ValidBucket returns whether or not
// bucket is a valid bucket name.
//
// See https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
//
// Note: ValidBucket does not allow '.' characters,
// since bucket names containing dots are not accessible
// over HTTPS. (AWS docs say "not recommended for uses other than static website hosting.")
func ValidBucket(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 {
		return false
	}
	if strings.HasPrefix(bucket, "xn--") {
		return false
	}
	if strings.HasSuffix(bucket, "-s3alias") {
		return false
	}
	for i := 0; i < len(bucket); i++ {
		if bucket[i] >= 'a' && bucket[i] <= 'z' {
			continue
		}
		if bucket[i] >= '0' && bucket[i] <= '9' {
			continue
		}
		if i > 0 && i < len(bucket)-1 {
			if bucket[i] == '-' {
				continue
			}
			if bucket[i] == '.' && bucket[i-1] != '.' {
				continue
			}
		}
		return false
	}
	return true
}

// Reader presents a read-only view of an S3 object
type Reader struct {
	// Key is the sigining key that
	// Reader uses to make HTTP requests.
	// The key may have to be refreshed
	// every so often (see aws.SigningKey.Expired)
	Key *aws.SigningKey `xml:"-"`

	// Client is the HTTP client used to
	// make HTTP requests. By default it is
	// populated with DefaultClient, but
	// it may be set to any reasonable http client
	// implementation.
	Client *http.Client `xml:"-"`

	// ETag is the ETag of the object in S3
	// as returned by listing or a HEAD operation.
	ETag string `xml:"ETag"`
	// LastModified is the object's LastModified time
	// as returned by listing or a HEAD operation.
	LastModified time.Time `xml:"LastModified"`
	// Size is the object size in bytes.
	// It is populated on Open.
	Size int64 `xml:"Size"`
	// Bucket is the S3 bucket holding the object.
	Bucket string `xml:"-"`
	// Path is the S3 object key.
	Path string `xml:"Key"`
}

// rawURI produces a URI with a pre-escaped path+query string
func rawURI(k *aws.SigningKey, bucket string, query string) string {
	endPoint := k.BaseURI
	if endPoint == "" {
		// use virtual-host style if the bucket is compatible
		// (fallback to path-style if not)
		if strings.IndexByte(bucket, '.') < 0 {
			return "https://" + bucket + ".s3." + k.Region + ".amazonaws.com" + "/" + query
		} else {
			return "https://s3." + k.Region + ".amazonaws.com" + "/" + bucket + "/" + query
		}
	}
	return endPoint + "/" + bucket + "/" + query
}

// perform S3-specific path escaping;
// all the special characters are turned
// into their quoted bits, but we turn %2F
// back into / because AWS accepts those
// as part of the URI
func almostPathEscape(s string) string {
	return strings.ReplaceAll(queryEscape(s), "%2F", "/")
}

func queryEscape(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

// uri produces a URI by path-escaping the object string
// and passing it to rawURI (see also almostPathEscape)
func uri(k *aws.SigningKey, bucket, object string) string {
	return rawURI(k, bucket, almostPathEscape(object))
}

// URL returns a signed URL for a bucket and object
// that can be used directly with http.Get.
func URL(k *aws.SigningKey, bucket, object string) (string, error) {
	if !ValidBucket(bucket) {
		return "", badBucket(bucket)
	}
	return k.SignURL(uri(k, bucket, object), 1*time.Hour)
}

// Stat performs a HEAD on an S3 object
// and returns an associated Reader.
func Stat(k *aws.SigningKey, bucket, object string) (*Reader, error) {
	r := new(Reader)
	body, err := r.open(k, bucket, object, false)
	if body != nil {
		body.Close()
	}
	return r, err
}

// NewFile constructs a File that points to the given
// bucket, object, etag, and file size. The caller is
// assumed to have correctly determined these attributes
// in advance; this call does not perform any I/O to verify
// that the provided object exists or has a matching ETag
// and size.
func NewFile(k *aws.SigningKey, bucket, object, etag string, size int64) *File {
	return &File{
		Reader: Reader{
			Key:    k,
			Bucket: bucket,
			Path:   object,
			ETag:   etag,
			Size:   size,
		},
		ctx: context.Background(),
	}
}

// Open performs a GET on an S3 object
// and returns the associated File.
func Open(k *aws.SigningKey, bucket, object string, contents bool) (*File, error) {
	f := new(File)
	err := f.open(k, bucket, object, contents)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func flakyDo(cl *http.Client, req *http.Request) (*http.Response, error) {
	hasBody := req.Body != nil
	if cl == nil {
		cl = &DefaultClient
	}
	res, err := cl.Do(req)
	if err == nil && (res.StatusCode != 500 && res.StatusCode != 503) {
		return res, err
	}
	if hasBody && req.GetBody == nil {
		// can't re-do this request because
		// we can't rewind the Body reader
		return res, err
	}
	if res != nil {
		res.Body.Close()
	}
	if hasBody {
		req.Body, err = req.GetBody()
		if err != nil {
			return nil, fmt.Errorf("req.GetBody: %w", err)
		}
	}
	return cl.Do(req)
}

func (f *File) open(k *aws.SigningKey, bucket, object string, contents bool) error {
	body, err := f.Reader.open(k, bucket, object, true)
	if err != nil {
		if body != nil {
			body.Close()
		}
		return err
	}
	if !contents {
		body.Close()
		body = nil
	}
	f.body = body
	f.ctx = context.Background()
	return nil
}

func (r *Reader) open(k *aws.SigningKey, bucket, object string, contents bool) (io.ReadCloser, error) {
	if !ValidBucket(bucket) {
		return nil, badBucket(bucket)
	}
	method := http.MethodHead
	if contents {
		method = http.MethodGet
	}
	req, err := http.NewRequest(method, uri(k, bucket, object), nil)
	if err != nil {
		return nil, err
	}
	k.SignV4(req, nil)

	// FIXME: configurable http.Client here?
	res, err := flakyDo(&DefaultClient, req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode == 404 {
		return res.Body, &fs.PathError{
			Op:   "open",
			Path: "s3://" + bucket + "/" + object,
			Err:  fs.ErrNotExist,
		}
	}
	if res.StatusCode != 200 {
		// NOTE: we can't extractMessage() here, because HEAD
		// errors do not produce a response with an error message
		return res.Body, fmt.Errorf("s3.Open: HEAD returned %s", res.Status)
	}
	if res.ContentLength < 0 {
		return res.Body, fmt.Errorf("s3.Open: content length %d invalid", res.ContentLength)
	}
	lm, _ := time.Parse(time.RFC1123, res.Header.Get("LastModified"))
	*r = Reader{
		Key:          k,
		Client:       &DefaultClient,
		ETag:         res.Header.Get("ETag"),
		LastModified: lm,
		Size:         res.ContentLength,
		Bucket:       bucket,
		Path:         object,
	}
	return res.Body, nil
}

// WriteTo implements io.WriterTo
func (r *Reader) WriteTo(w io.Writer) (int64, error) {
	req, err := http.NewRequest("GET", uri(r.Key, r.Bucket, r.Path), nil)
	if err != nil {
		return 0, err
	}
	r.Key.SignV4(req, nil)

	res, err := flakyDo(r.Client, req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return 0, fmt.Errorf("s3.Reader.WriteTo: status %s %q", res.Status, extractMessage(res.Body))
	}
	return io.Copy(w, res.Body)
}

// RangeReader produces an io.ReadCloser that reads
// bytes in the range from [off, off+width)
//
// It is the caller's responsibility to call Close()
// on the returned io.ReadCloser.
func (r *Reader) RangeReader(off, width int64) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", uri(r.Key, r.Bucket, r.Path), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+width-1))
	req.Header.Set("If-Match", r.ETag)
	r.Key.SignV4(req, nil)
	res, err := flakyDo(r.Client, req)
	if err != nil {
		return nil, err
	}
	switch res.StatusCode {
	default:
		defer res.Body.Close()
		return nil, fmt.Errorf("s3.Reader.RangeReader: status %s %q", res.Status, extractMessage(res.Body))
	case http.StatusPreconditionFailed:
		res.Body.Close()
		return nil, ErrETagChanged
	case http.StatusNotFound:
		res.Body.Close()
		return nil, &fs.PathError{Op: "read", Path: r.Path, Err: fs.ErrNotExist}
	case http.StatusPartialContent, http.StatusOK:
		// okay; fallthrough
	}
	return res.Body, nil
}

// ReadAt implements io.ReaderAt
func (r *Reader) ReadAt(dst []byte, off int64) (int, error) {
	rd, err := r.RangeReader(off, int64(len(dst)))
	if err != nil {
		return 0, err
	}
	defer rd.Close()
	return io.ReadFull(rd, dst)
}

// BucketRegion returns the region associated
// with the given bucket.
func BucketRegion(k *aws.SigningKey, bucket string) (string, error) {
	if !ValidBucket(bucket) {
		return "", badBucket(bucket)
	}
	if k.BaseURI != "" {
		return k.Region, nil
	}
	uri := rawURI(k, bucket, "?location=")
	req, err := http.NewRequest(http.MethodGet, uri, nil)
	if err != nil {
		return "", err
	}
	k.SignV4(req, nil)
	res, err := flakyDo(&DefaultClient, req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode == 403 {
		return k.Region, nil
	}
	if res.StatusCode != 200 {
		return "", fmt.Errorf("s3.BucketRegion: %s %q", res.Status, extractMessage(res.Body))
	}
	var ret string
	err = xml.NewDecoder(res.Body).Decode(&ret)
	if err != nil {
		return "", fmt.Errorf("s3.BucketRegion: decoding response: %w", err)
	}
	if ret == "" || ret == "null" {
		return "us-east-1", nil
	}
	return ret, nil
}

// DeriveForBucket can be passed to aws.AmbientCreds
// as a DeriveFn that automatically re-derives keys
// so that they apply to the region in which the
// given bucket lives.
func DeriveForBucket(bucket string) aws.DeriveFn {
	return func(baseURI, id, secret, token, region, service string) (*aws.SigningKey, error) {
		if !ValidBucket(bucket) {
			return nil, badBucket(bucket)
		}
		if service != "s3" {
			return nil, fmt.Errorf("s3.DeriveForBucket: expected service \"s3\"; got %q", service)
		}
		k := aws.DeriveKey(baseURI, id, secret, region, service)
		k.Token = token
		bregion, err := BucketRegion(k, bucket)
		if err != nil {
			return nil, err
		}
		if bregion == region {
			return k, nil
		}
		k = aws.DeriveKey(baseURI, id, secret, bregion, service)
		k.Token = token
		return k, nil
	}
}
