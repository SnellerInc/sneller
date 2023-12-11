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

package vm_test

import (
	"bytes"
	"testing"

	"github.com/SnellerInc/sneller/vm"
)

// Test that combined TeeWriters call all
// finalizers appropriately. See issue #1632.
func TestTeeWriterCombine(t *testing.T) {
	var w bytes.Buffer
	final1 := false
	t1 := vm.NewTeeWriter(&w, func(int64, error) { final1 = true })
	final2 := false
	t2 := vm.NewTeeWriter(t1, func(int64, error) { final2 = true })
	t2.Close()
	t2.Write([]byte("foo")) // cover nil writer
	if !final1 {
		t.Error("t1 finalizer not called")
	}
	if !final2 {
		t.Error("t2 finalizer not called")
	}
}
