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

package s3

import (
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/aws"
)

type testRoundTripper struct {
	t      *testing.T
	expect struct {
		method   string
		uri      string
		body     string
		skipBody bool
		headers  []string
	}
	response struct {
		code    int
		body    string
		headers http.Header
	}
}

var errUnexpected = errors.New("unexpected round-trip request")

func (t *testRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		defer req.Body.Close()
	}
	if req.Method != t.expect.method {
		t.t.Errorf("method %q expected; got %q", t.expect.method, req.Method)
		return nil, errUnexpected
	}
	if uri := req.URL.RequestURI(); uri != t.expect.uri {
		t.t.Errorf("uri %q expected; got %q", t.expect.uri, uri)
		return nil, errUnexpected
	}
	for i := range t.expect.headers {
		if req.Header.Get(t.expect.headers[i]) == "" {
			t.t.Errorf("header %q missing", t.expect.headers[i])
			return nil, errUnexpected
		}
	}
	if !t.expect.skipBody && t.expect.body != "" {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		if string(body) != t.expect.body {
			t.t.Errorf("expected body %q; got %q", t.expect.body, body)
			return nil, errUnexpected
		}
	}

	res := &http.Response{
		StatusCode:    t.response.code,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Body:          io.NopCloser(strings.NewReader(t.response.body)),
		ContentLength: int64(len(t.response.body)),
		Header:        t.response.headers,
	}
	return res, nil
}

// Test an upload session against request/response
// strings from the documentation
func TestUpload(t *testing.T) {
	trt := &testRoundTripper{t: t}
	up := Uploader{
		Key:    aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3"),
		Client: &http.Client{Transport: trt},
		Bucket: "the-bucket",
		Object: "the-object",
	}

	trt.expect.method = "POST"
	trt.expect.uri = "/the-object?uploads="
	trt.expect.headers = []string{"Authorization"}
	trt.expect.body = ""
	trt.response.code = 200
	trt.response.headers = make(http.Header)
	trt.response.headers.Set("Content-Type", "application/xml")
	trt.response.body = `<InitiateMultipartUploadResult>
<Bucket>the-bucket</Bucket>
<Key>the-object</Key>
<UploadId>the-upload-id</UploadId>
</InitiateMultipartUploadResult>`

	err := up.Start()
	if err != nil {
		t.Fatal(err)
	}

	if up.ID() != "the-upload-id" {
		t.Errorf("bad upload id %q", up.ID())
	}

	// upload two parts in reverse order,
	// so we can test that the final
	// POST merges the parts in-order

	trt.expect.method = "PUT"
	trt.expect.uri = "/the-object?partNumber=2&uploadId=the-upload-id"
	trt.expect.skipBody = true
	trt.response.body = ""
	trt.response.headers = make(http.Header)
	trt.response.headers.Set("ETag", "the-ETag-2")
	part := make([]byte, MinPartSize+1)
	err = up.Upload(2, part)
	if err != nil {
		t.Fatal(err)
	}
	if up.CompletedParts() != 1 {
		t.Errorf("%d completed parts?", up.CompletedParts())
	}
	trt.expect.uri = "/the-object?partNumber=1&uploadId=the-upload-id"
	trt.response.headers.Set("ETag", "the-ETag-1")
	err = up.Upload(1, part)
	if err != nil {
		t.Fatal(err)
	}

	trt.expect.method = "POST"
	trt.expect.uri = "/the-object?uploadId=the-upload-id"
	trt.expect.headers = []string{"Authorization", "Content-Type"}
	trt.expect.skipBody = false
	trt.expect.body = `<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Part><PartNumber>1</PartNumber><ETag>the-ETag-1</ETag></Part><Part><PartNumber>2</PartNumber><ETag>the-ETag-2</ETag></Part></CompleteMultipartUpload>`
	trt.response.body = `<CompleteMultipartUploadResult>
<Location>the-bucket.s3.amazonaws.com/the-object</Location>
<Bucket>the-bucket</Bucket>
<Key>the-object</Key>
<ETag>the-final-ETag</ETag>
</CompleteMultipartUploadResult>`
	trt.response.headers = make(http.Header)
	trt.response.headers.Set("Content-Type", "application/xml")
	err = up.Close(nil)
	if err != nil {
		t.Fatal(err)
	}
	if up.ETag() != "the-final-ETag" {
		t.Errorf("unexpected ETag %q", up.ETag())
	}

	// Abort shouldn't do anything here:
	if err := up.Abort(); err != nil {
		t.Errorf("abort: %s", err)
	}

	// rewind the state and try the error case
	up.finished = false
	up.finalETag = ""
	trt.response.body = `<Error>
<Code>InternalError</Code>
<Message>injected error message</Message>
</Error>`
	trt.response.headers = make(http.Header)
	trt.response.headers.Set("Content-Type", "application/xml")

	err = up.Close(nil)
	if err == nil {
		t.Fatal("no error when <Error/> body returned")
	}
	if !strings.Contains(err.Error(), "injected error message") {
		t.Fatalf("unexpected error message %q", err)
	}

	// now test Abort
	trt.expect.method = "DELETE"
	trt.expect.headers = []string{"Authorization"}
	trt.expect.uri = "/the-object?uploadId=the-upload-id"
	trt.expect.body = ""
	trt.response.body = ""
	trt.response.code = 204
	trt.response.headers = make(http.Header)
	err = up.Abort()
	if err != nil {
		t.Errorf("abort: %s", err)
	}
}
