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

package vm

import (
	"os"
	"testing"
)

func TestLimit(t *testing.T) {
	buf, err := os.ReadFile("../testdata/parking.10n")
	if err != nil {
		t.Fatal(err)
	}
	amounts := []int{
		0, 1, 15, 16, 17, 100, 127,
		1021, 1022, 1023,
	}
	for i := range amounts {
		var dst QueryBuffer
		l := NewLimit(int64(amounts[i]), &dst)
		s, err := NewProjection(selection("Ticket as t"), l)
		if err != nil {
			t.Fatal(err)
		}
		err = CopyRows(s, buftbl(buf), 1)
		if err != nil {
			t.Errorf("LIMIT %d: %s", amounts[i], err)
			continue
		}
		b := dst.Bytes()
		skipok(b, t)
		t.Logf("LIMIT %d: %d bytes output", amounts[i], len(dst.Bytes()))
		out := len(structures(dst.Bytes()))
		if out != amounts[i] {
			t.Errorf("len(out)=%d, LIMIT %d ?", out, amounts[i])
		}
	}
}
