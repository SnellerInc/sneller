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

package ion

import (
	"bytes"
	"testing"

	"github.com/SnellerInc/sneller/date"
)

func TestWriteTruncatedTime(t *testing.T) {
	ts := date.Date(2021, 8, 22, 14, 42, 32, 0)

	testcases := []struct {
		trunc TimeTrunc
		ion   []byte
	}{
		{trunc: TruncToSecond,
			ion: []byte{0x68, 0x80, 0x0f, 0xe5, 0x88, 0x96, 0x8e, 0xaa, 0xa0}},
		{trunc: TruncToMinute,
			ion: []byte{0x67, 0x80, 0x0f, 0xe5, 0x88, 0x96, 0x8e, 0xaa}},
		{trunc: TruncToHour,
			ion: []byte{0x66, 0x80, 0x0f, 0xe5, 0x88, 0x96, 0x8e}},
		{trunc: TruncToDay,
			ion: []byte{0x65, 0x80, 0x0f, 0xe5, 0x88, 0x96}},
		{trunc: TruncToMonth,
			ion: []byte{0x64, 0x80, 0x0f, 0xe5, 0x88}},
		{trunc: TruncToYear,
			ion: []byte{0x63, 0x80, 0x0f, 0xe5}},
	}

	var buf Buffer
	for i := range testcases {
		buf.Reset()
		buf.WriteTruncatedTime(ts, testcases[i].trunc)
		if !bytes.Equal(buf.Bytes(), testcases[i].ion) {
			t.Logf("got:      % 02x", buf.Bytes())
			t.Logf("expected: % 02x", testcases[i].ion)
			t.Errorf("case #%d: wrongly encoded ion", i)
		}
	}
}

func TestWriteTruncatedTimeMatchesWriteTime(t *testing.T) {
	ts := date.Date(2021, 8, 22, 14, 42, 32, 0)

	var buf1 Buffer
	buf1.WriteTime(ts)

	var buf2 Buffer
	buf2.WriteTruncatedTime(ts, TruncToSecond)

	if !bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
		if !bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
			t.Logf("got:      % 02x", buf1.Bytes())
			t.Logf("expected: % 02x", buf2.Bytes())
			t.Errorf("wrongly encoded ion")
		}
	}
}
