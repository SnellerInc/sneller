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

//go:build amd64
// +build amd64

package ion

import (
	"bytes"
	"testing"
)

func TestIssue419(t *testing.T) {
	tcs := []struct {
		value    uint
		encoding []byte
	}{
		{value: 0x58, encoding: []byte{0xd8}},
		{value: 0x43, encoding: []byte{0xc3}},
		{value: 0x43 << 7, encoding: []byte{0x43, 0x80}},
	}
	var buf Buffer
	for i := range tcs {
		buf.Reset()
		buf.putuv(tcs[i].value)
		if !bytes.Equal(buf.Bytes(), tcs[i].encoding) {
			t.Errorf("got %x; want %x", buf.Bytes(), tcs[i].encoding)
		}
	}
}
