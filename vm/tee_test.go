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
