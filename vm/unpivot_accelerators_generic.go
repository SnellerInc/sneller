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
