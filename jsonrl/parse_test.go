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

package jsonrl_test

import (
	"compress/gzip"
	"io"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/jsonrl"
)

func TestConvertTestdata(t *testing.T) {
	dir := os.DirFS("./testdata")
	walk := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path == "fuzz" {
				// ignore testdata/fuzz
				return fs.SkipDir
			}
			return nil
		}
		t.Run(path, func(t *testing.T) {
			f, err := dir.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			r := (io.Reader)(f)
			if strings.HasSuffix(path, ".zst") {
				d, err := zstd.NewReader(f)
				if err != nil {
					t.Fatal(err)
				}
				defer d.Close()
				r = d
			} else if strings.HasSuffix(path, ".gz") {
				d, err := gzip.NewReader(f)
				if err != nil {
					t.Fatal(err)
				}
				defer d.Close()
				r = d
			}
			cn := ion.Chunker{
				Align: 1024 * 1024,
				W:     io.Discard,
			}
			err = jsonrl.Convert(r, &cn, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
		})
		return nil
	}
	fs.WalkDir(dir, ".", walk)
}
