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

//go:build !amd64
// +build !amd64

package vm

import (
	"math/bits"
	"unsafe"

	"github.com/SnellerInc/sneller/ints"
	"github.com/SnellerInc/sneller/ion"
)

func copyVMrefs(p *[]vmref, q *vmref, n int) {
	// The caller is responsible for ensuring there's enough space in *p; the accelerator does not validate this.
	m := len(*p)
	k := m + n
	*p = (*p)[:k]
	copy((*p)[m:], unsafe.Slice(q, n))
}

func fillVMrefs(p *[]vmref, v vmref, n int) {
	// The caller is responsible for ensuring there's enough space in *p; the accelerator does not validate this.
	m := len(*p)
	k := m + n
	*p = (*p)[:k]
	for i := m; i < k; i++ {
		(*p)[i] = v
	}
}

func unpivotAtDistinctDeduplicate(rows []vmref, vmbase uintptr, bitvector *uint) {
	// The caller is responsible for ensuring there's enough space in the bitvector; the accelerator does not validate this.
	sbv := unsafe.Slice(bitvector, (1<<21)/bits.UintSize)
	for _, row := range rows {
		if row[1] == 0 {
			continue
		}
		data := row.mem()
		for len(data) > 0 {
			sym, rest, err := ion.ReadLabel(data)
			if err != nil {
				panic(err)
			}

			vsize := ion.SizeOf(rest)
			data = rest[vsize:]
			ints.SetBit(sbv, sym)
		}
	}
}
