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

package vm

import (
	"golang.org/x/sys/cpu"
)

const (
	avx512highestlevel uint8 = iota
	avx512level1
	avx512level2
)

// avx512level determines the current CPU's level of AVX512 instructions.
func avx512level() uint8 {
	if cpu.X86.HasAVX512VBMI &&
		cpu.X86.HasAVX512VBMI2 &&
		cpu.X86.HasAVX512VPOPCNTDQ &&
		cpu.X86.HasAVX512IFMA &&
		cpu.X86.HasAVX512BITALG &&
		cpu.X86.HasAVX512VAES &&
		cpu.X86.HasAVX512GFNI &&
		cpu.X86.HasAVX512VPCLMULQDQ {
		return avx512level2
	}

	return avx512level1
}

// setavx512level sets SSA instructions to use opcodes from given AVX512 ISA level.
func setavx512level(level uint8) {
	switch level {
	case avx512highestlevel:
		setavx512level(avx512level())

	case avx512level1:
		initssadefs()

	case avx512level2:
		patchssadefs(patchAVX512Level2)
	}
}

func initssadefs() {
	copy(ssainfo[:], _ssainfo[:])
}

func patchssadefs(repl []opreplace) {
	if len(repl) == 0 {
		return
	}

	lookup := make(map[bcop]bcop)
	for i := range repl {
		r := &repl[i]
		lookup[r.from] = r.to
	}

	for i := range _ssainfo {
		// Note: we lookup in the _ssainfo and modify ssainfo
		bc, ok := lookup[_ssainfo[i].bc]
		if ok {
			ssainfo[i].bc = bc
		}
	}
}
