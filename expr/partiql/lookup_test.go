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

package partiql

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"sync"
	"testing"
)

var (
	testLookupMutex       sync.Mutex
	testLookupIdentifiers [][]byte
)

func BenchmarkLookupKeyword(b *testing.B) {
	testLoadLookupInputs(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, w := range testLookupIdentifiers {
			_, _ = lookupKeyword(w)
		}
	}
}

func testLoadLookupInputs(t testing.TB) {
	testLookupMutex.Lock()
	defer testLookupMutex.Unlock()

	if len(testLookupIdentifiers) != 0 {
		return
	}

	f, err := os.Open("./testdata/keywords.txt.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()

	buf, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}

	lines := bytes.Split(buf, []byte{'\n'})
	for _, line := range lines {
		if word, ok := bytes.CutPrefix(line, []byte("K: ")); ok {
			testLookupIdentifiers = append(testLookupIdentifiers, word)
		} else if word, ok := bytes.CutPrefix(line, []byte("A: ")); ok {
			testLookupIdentifiers = append(testLookupIdentifiers, word)
		}
	}
}
