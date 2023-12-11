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
