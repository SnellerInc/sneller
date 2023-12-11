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

package jsonrl

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

func TestCloudtrail(t *testing.T) {
	testFile := func(t *testing.T, p string) {
		f, err := os.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		cn := &ion.Chunker{
			Align: 8192,
			W:     io.Discard,
		}
		err = ConvertCloudtrail(f, cn, nil)
		if err != nil {
			t.Fatal(err)
		}
		err = cn.Flush()
		if err != nil {
			t.Fatal(err)
		}
	}
	walk := func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(p, ".json") {
			t.Run(p, func(t *testing.T) {
				testFile(t, p)
			})
		}
		return nil
	}
	err := filepath.WalkDir("./testdata/cloudtrail", walk)
	if err != nil {
		t.Fatal(err)
	}
}
