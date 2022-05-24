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
			err = jsonrl.Convert(r, &cn, nil)
			if err != nil {
				t.Fatal(err)
			}
		})
		return nil
	}
	fs.WalkDir(dir, ".", walk)
}
