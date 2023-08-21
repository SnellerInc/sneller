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

package sneller

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os/exec"
	"slices"
	"sync"
	"testing"
)

func TestImports(t *testing.T) {
	lines, err := exec.Command("go", "list", "./...").CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	type goPackage struct {
		Imports []string `json:"Imports"`
	}
	failed := make(chan string, 1)
	var wg sync.WaitGroup
	s := bufio.NewScanner(bytes.NewReader(lines))
	for s.Scan() {
		wg.Add(1)
		go func(pkgname string) {
			defer wg.Done()
			desc, err := exec.Command("go", "list", "-json", pkgname).CombinedOutput()
			if err != nil {
				panic(err)
			}
			var pkg goPackage
			err = json.Unmarshal(desc, &pkg)
			if err != nil {
				panic(err)
			}
			if slices.Contains(pkg.Imports, "testing") {
				failed <- pkgname
			}
		}(s.Text())
	}
	go func() {
		wg.Wait()
		close(failed)
	}()
	for name := range failed {
		t.Errorf("package %s imports \"testing\"", name)
	}
}
