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
		err = ConvertCloudtrail(f, cn)
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
