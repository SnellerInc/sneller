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

package aws

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"testing"
	"time"
)

func init() {
	faketime = true

	fn, err := time.Parse(longFormat, "20150830T123600Z")
	if err != nil {
		panic(err)
	}
	fakenow = fn.Local() // set non-UTC time, just to check that we fix it
}

// setnow sets fakenow and resets it at cleanup
func setnow(t *testing.T, tm time.Time) {
	old := fakenow
	t.Cleanup(func() { fakenow = old })
	fakenow = tm
}

// test against the example in the documentation
func TestCanonical(t *testing.T) {
	// use these headers
	sigheaders = []string{"content-type", "host", "x-amz-date"}
	defer func() {
		sigheaders = []string{"host"}
	}()

	req, err := http.NewRequest("GET", "https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08 HTTP/1.1", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	req.Header.Set("X-Amz-Date", "20150830T123600Z")
	req.Header.Set("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

	var out bytes.Buffer
	canonical(&out, req)
	outstr := out.String()
	const want = `GET
/
Action=ListUsers&Version=2010-05-08
content-type:application/x-www-form-urlencoded; charset=utf-8
host:iam.amazonaws.com
x-amz-date:20150830T123600Z

content-type;host;x-amz-date
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
	if outstr != want {
		t.Logf("got:\n%s", outstr)
		t.Error("didn't match")
	}
	h := sha256.Sum256(out.Bytes())
	hstr := hex.EncodeToString(h[:])
	if hstr != "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59" {
		t.Errorf("got hash %s ??", hstr)
	}
}

// test from
// https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
func TestToSign(t *testing.T) {
	const want = `AWS4-HMAC-SHA256
20150830T123600Z
20150830/us-east-1/iam/aws4_request
f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59`

	var dst bytes.Buffer
	s := &SigningKey{
		Region:  "us-east-1",
		Service: "iam",
	}
	s.tosign(&dst, time.Date(2015, time.August, 30, 12, 36, 0, 0, time.UTC), "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59")

	if dst.String() != want {
		t.Errorf("got:\n%s", dst.String())
	}
}

// test from
// https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
func TestSigningKey(t *testing.T) {
	when := time.Date(2015, time.August, 30, 12, 36, 0, 0, time.UTC)
	k := derive("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", when, "us-east-1", "iam")
	x := hex.EncodeToString(k)
	const want = "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
	if x != want {
		t.Errorf("got %s", x)
	}

	const testvec = `AWS4-HMAC-SHA256
20150830T123600Z
20150830/us-east-1/iam/aws4_request
f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59`

	sk := DeriveKey("", "", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "us-east-1", "iam")

	var dst [2 * sha256.Size]byte
	sk.sign([]byte(testvec), dst[:], when)
	const wantsig = "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
	if got := string(dst[:]); got != wantsig {
		t.Errorf("got sig %s", got)
	}
}

// See https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html#query-string-auth-v4-signing-example
func TestSignURL(t *testing.T) {
	// derive the key in the preceding day
	fn, err := time.Parse(longFormat, "20130523T010203Z")
	if err != nil {
		t.Fatal(err)
	}
	setnow(t, fn)
	input := "https://examplebucket.s3.amazonaws.com/test.txt"
	k := DeriveKey("", "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1", "s3")

	// change the day to "tomorrow" and confirm
	// that the signature is still produced correctly
	fn, err = time.Parse(longFormat, "20130524T000000Z")
	if err != nil {
		t.Fatal(err)
	}
	setnow(t, fn)
	ret, err := k.SignURL(input, 86400*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	want := "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	if ret != want {
		t.Logf("got : %s", ret)
		t.Logf("want: %s", want)
		t.Error("didn't sign correctly")
	}
}
