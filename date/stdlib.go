// Copyright (c) 2009 The Go Authors. All rights reserved.
// Copyright (c) 2022 Sneller, Inc.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//    * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//    * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//    * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package date

import "errors"

func isleap(year int) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

func norm(hi, lo, base int) (nhi, nlo int) {
	if lo < 0 {
		n := (-lo-1)/base + 1
		hi -= n
		lo += n * base
	}
	if lo >= base {
		n := lo / base
		hi += n
		lo -= n * base
	}
	return hi, lo
}

// MarshalJSON implements json.Marshaler.
func (t Time) MarshalJSON() ([]byte, error) {
	if t.Year() >= 10000 {
		return nil, errors.New("date.MarshalJSON: year outside of range [0,9999]")
	}
	n := len("0000-00-00T00:00:00Z")
	if t.ns != 0 {
		n += len(".999999999")
	}
	b := make([]byte, 0, n+2)
	b = append(b, '"')
	b = t.AppendRFC3339Nano(b)
	b = append(b, '"')
	return b, nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (t *Time) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	if len(b) == 0 || b[0] != '"' {
		return errors.New("date.UnmarshalJSON: expected a string")
	}
	var ok bool
	*t, ok = Parse(b[1 : len(b)-1])
	if !ok {
		return errors.New("date.UnmarshalJSON: failed to parse")
	}
	return nil
}

// appendInt appends the decimal form of x to b and
// returns the result. If the decimal form (excluding
// sign) is shorter than width, the result is padded
// with leading 0's. Duplicates functionality in
// strconv, but avoids dependency.
//
// If trim is true, trailing zeroes will not be
// included.
func appendInt(b []byte, x int, width int, trim bool) []byte {
	u := uint(x)
	if x < 0 {
		b = append(b, '-')
		u = uint(-x)
	}

	// Assemble decimal in reverse order.
	var buf [20]byte
	i := len(buf)
	for u >= 10 {
		q := u / 10
		if trim = trim && u == q*10; trim {
			width-- // avoid extra padding
		} else {
			i--
			buf[i] = byte('0' + u - q*10)
		}
		u = q
	}
	i--
	buf[i] = byte('0' + u)

	// Add 0-padding.
	for w := len(buf) - i; w < width; w++ {
		b = append(b, '0')
	}

	return append(b, buf[i:]...)
}
