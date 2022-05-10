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

// Package aws is a lightweight implementation
// of the AWS API signature algorithms.
// Currently only the Version 4 algorithm is supported.
package aws

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var (
	faketime bool = false
	fakenow  time.Time
)

func signtime() time.Time {
	if faketime {
		return fakenow
	}
	return time.Now()
}

const (
	longFormat  = "20060102T150405Z"
	shortFormat = "20060102"
)

// note: this list needs to be alphabetically sorted
var sigheaders = []string{
	"host",
	"x-amz-content-sha256",
	"x-amz-copy-source",
	"x-amz-copy-source-if-match",
	"x-amz-copy-source-range",
	"x-amz-date",
	"x-amz-security-token",
}

func (s *SigningKey) toscope(dst *bytes.Buffer, now time.Time) {
	dst.WriteString(now.Format(shortFormat))
	dst.WriteByte('/')
	dst.WriteString(s.Region)
	dst.WriteByte('/')
	dst.WriteString(s.Service)
	dst.WriteString("/aws4_request")
}

// string to sign
// see
// https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
func (s *SigningKey) tosign(dst *bytes.Buffer, now time.Time, reqhash string) {
	dst.WriteString("AWS4-HMAC-SHA256\n")
	// date value
	dst.WriteString(now.Format(longFormat))
	dst.WriteByte('\n')
	// request scope
	s.toscope(dst, now)
	dst.WriteByte('\n')
	// request hash
	dst.WriteString(reqhash)
}

// write the 'canonical request' into dst
// see https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
func canonical(dst *bytes.Buffer, req *http.Request) {
	dst.WriteString(req.Method)
	dst.WriteByte('\n')

	uri := req.URL.EscapedPath()
	if uri == "" {
		uri = "/"
	}
	dst.WriteString(uri)
	dst.WriteByte('\n')

	querystr := strings.TrimSuffix(req.URL.RawQuery, " HTTP/1.1")
	dst.WriteString(querystr)
	dst.WriteByte('\n')

	// we are *required* to signed the host header;
	// everything else is optional (except for HTTP/2,
	// which requires the authority header)
	if req.Header.Get("Host") == "" {
		req.Header.Set("Host", req.URL.Host)
	}

	var bodyhash string
	for i := range sigheaders {
		h := sigheaders[i]
		hdr := req.Header.Get(h)
		if hdr == "" {
			continue
		}
		if h == "x-amz-content-sha256" {
			bodyhash = hdr
		}
		dst.WriteString(h)
		dst.WriteByte(':')
		dst.WriteString(hdr)
		dst.WriteByte('\n')
	}
	dst.WriteByte('\n')

	// signed headers string
	for i := range sigheaders {
		if req.Header.Get(sigheaders[i]) == "" {
			continue
		}
		if i != 0 {
			dst.WriteByte(';')
		}
		dst.WriteString(sigheaders[i])
	}
	dst.WriteByte('\n')
	// the value to be hashed here
	// needs to match the header,
	// even if it is the string UNSIGNED-PAYLOAD
	if bodyhash == "" {
		bodyhash = req.Header.Get("x-amz-content-sha256")
	}
	dst.WriteString(bodyhash)
}

// SignV4 signs an http.Request using the
// AWS S3 V4 Authentication scheme.
//
// The body of the request will be set to 'body'
// and the Authorization header will be populated
// with the necessary authorization contents.
// The X-Amz-Date header will also be set to
// an appropriate value.
//
// BUGS: the encoded query string must have
// the query parameters in sorted order already.
// Query parameters with no arguments must include
// a bare trailing '=' so that they are canonicalized
// correctly.
func (s *SigningKey) SignV4(req *http.Request, body []byte) {
	var buf bytes.Buffer

	now := signtime().UTC()
	req.Header.Set("x-amz-date", now.Format(longFormat))
	if s.Token != "" {
		req.Header.Set("x-amz-security-token", s.Token)
	}

	// canonical() uses the value we set here
	// as the hash of the body
	if body == nil {
		req.Header.Set("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	} else {
		// note: could also just calculate the sha256 of the payload,
		// but really we should just use HTTPS, which provides
		// better integrity guarantees anyway...
		req.Header.Set("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
	}

	// compute signature
	// and stick it into hexbuf
	var hexbuf [2 * sha256.Size]byte
	canonical(&buf, req)
	h := sha256.Sum256(buf.Bytes())
	buf.Reset()
	s.tosign(&buf, now, hex.EncodeToString(h[:]))
	s.sign(buf.Bytes(), hexbuf[:], now)

	buf.Reset()
	buf.WriteString("AWS4-HMAC-SHA256 Credential=")
	buf.WriteString(s.AccessKey)
	buf.WriteByte('/')
	s.toscope(&buf, now)
	buf.WriteString(", SignedHeaders=")
	for i := range sigheaders {
		if req.Header.Get(sigheaders[i]) == "" {
			continue
		}
		if i != 0 {
			buf.WriteByte(';')
		}
		buf.WriteString(sigheaders[i])
	}
	buf.WriteString(", Signature=")
	buf.Write(hexbuf[:])

	req.Header.Set("Authorization", buf.String())

	if body != nil {
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.ContentLength = int64(len(body))
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(body)), nil
		}
	} else {
		req.Body = nil
	}
}

// SignURL signs an HTTP request by creating
// a presigned URL string. The returned string
// is valid for only the specified duration.
func (s *SigningKey) SignURL(uri string, validfor time.Duration) (string, error) {
	now := signtime().UTC()
	u, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	host := u.Host
	var scope bytes.Buffer
	scope.WriteString(s.AccessKey)
	scope.WriteByte('/')
	s.toscope(&scope, now)

	q := u.Query()
	q.Add("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Add("X-Amz-Credential", scope.String())
	q.Add("X-Amz-Date", now.Format(longFormat))
	q.Add("X-Amz-Expires", strconv.FormatInt(int64(validfor/time.Second), 10))
	q.Add("X-Amz-SignedHeaders", "host")
	if s.Token != "" {
		q.Add("X-Amz-Security-Token", s.Token)
	}

	// TODO: if we have a SecurityToken, add it

	// build 'canonical request'
	// method
	var dst bytes.Buffer
	dst.WriteString("GET\n")
	// canonical URI
	dst.WriteString(u.EscapedPath())
	dst.WriteByte('\n')
	// canonical query string (url.Values.Encode() does the sorting)
	dst.WriteString(q.Encode())
	dst.WriteByte('\n')
	// canonical headers: just 'host:<host>'
	dst.WriteString("host:")
	dst.WriteString(host)
	dst.WriteByte('\n')
	// signed headers (just host) plus payload hash (UNSIGNED-PAYLOAD)
	dst.WriteString("\nhost\nUNSIGNED-PAYLOAD")

	var hexbuf [2 * sha256.Size]byte
	h := sha256.Sum256(dst.Bytes())
	dst.Reset()
	reqhash := hex.EncodeToString(h[:])
	s.tosign(&dst, now, reqhash)
	s.sign(dst.Bytes(), hexbuf[:], now)
	query := q.Encode() + "&X-Amz-Signature=" + string(hexbuf[:])
	// we're overriding the request scheme here to HTTPS,
	// since we're only signing the host header
	return u.Scheme + "://" + u.Host + u.EscapedPath() + "?" + query, nil
}

// SigningKey is a key that can be used
// to sign AWS service requests.
//
// Keys expire daily, as they use the current
// time in the derivation, so they must be refreshed
// regularly.
type SigningKey struct {
	BaseURI   string    // S3 base URI (empty is default AWS S3)
	Region    string    // AWS Region
	Service   string    // AWS Service
	AccessKey string    // AWS Access Key ID
	Token     string    // Token, if key is from STS
	Derived   time.Time // time token was derived

	// we only store the clamped secret
	// so that this object can't be repurposed
	// for other services / regions
	//
	// clamped0 is "today's" key when the
	// key was derived; clamped1 is "tomorrow's" key
	clamped0 []byte
	clamped1 []byte
}

func macinto(key, mem []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(mem)
	return h.Sum(key[:0])
}

func derive(secret string, when time.Time, region, service string) []byte {
	datestr := when.Format(shortFormat)
	k := []byte("AWS4" + secret)
	k = macinto(k, []byte(datestr))
	k = macinto(k, []byte(region))
	k = macinto(k, []byte(service))
	k = macinto(k, []byte("aws4_request"))
	return k
}

// DeriveKey derives a SigningKey that can be used
// to sign requests
func DeriveKey(baseURI, accessKey, secret, region, service string) *SigningKey {
	now := signtime().UTC()
	return &SigningKey{
		BaseURI:   baseURI,
		Region:    region,
		Service:   service,
		AccessKey: accessKey,
		Derived:   now,
		clamped0:  derive(secret, now, region, service),
		clamped1:  derive(secret, now.Add(24*time.Hour), region, service),
	}
}

func (s *SigningKey) pickKey(when time.Time) []byte {
	// if it is "tomorrow" then pick tomorrow's key
	if when.Sub(s.Derived) >= 24*time.Hour || when.Day() != s.Derived.Day() {
		return s.clamped1
	}
	return s.clamped0
}

func (s *SigningKey) sign(src, dst []byte, when time.Time) {
	var tmp [sha256.Size]byte
	m := hmac.New(sha256.New, s.pickKey(when))
	m.Write(src)
	hex.Encode(dst, m.Sum(tmp[:0]))
}
