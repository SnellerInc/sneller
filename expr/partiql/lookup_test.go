// Copyright (C) 2023 Sneller, Inc.
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
