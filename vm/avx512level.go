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
	"os"
	"strings"

	"golang.org/x/sys/cpu"
)

// OptimizationLevel describes which optimizations Sneller can use.
type OptimizationLevel uint32

const (
	// Don't use any optimizations.
	OptimizationLevelNone OptimizationLevel = iota

	// Use AVX-512 level 1 optimizations (baseline).
	//
	// Baseline AVX-512 requires F, BW, DQ, CD, and VL features.
	OptimizationLevelAVX512V1

	// Use AVX-512 level 2 optimizations (IceLake and Zen 4+).
	//
	// AVX-512 level 2 requires BITALG, GFNI, IFMA, VAES, VBMI, VBMI2,
	// VPCLMULQDQ, and VPOPCNTDQ.
	OptimizationLevelAVX512V2

	// Autodetect optimizations based on environment variable
	// (SNELLER_OPT_LEVEL) and detected CPU features.
	OptimizationLevelDetect = OptimizationLevel(0xFFFFFFFF)
)

const (
	optimizationLevelEnvVar = "SNELLER_OPT_LEVEL"
)

var globalOptimizationLevel OptimizationLevel

// optimizationLevelFromCPUFeatures determines the maximum optimization
// level that is supported by the CPU. If the CPU doesn't support AVX-512
// `OptimizationLevelNone` will be returned.
func optimizationLevelFromCPUFeatures() OptimizationLevel {
	if cpu.X86.HasAVX512VBMI &&
		cpu.X86.HasAVX512VBMI2 &&
		cpu.X86.HasAVX512VPOPCNTDQ &&
		cpu.X86.HasAVX512IFMA &&
		cpu.X86.HasAVX512BITALG &&
		cpu.X86.HasAVX512VAES &&
		cpu.X86.HasAVX512GFNI &&
		cpu.X86.HasAVX512VPCLMULQDQ {
		return OptimizationLevelAVX512V2
	}

	if cpu.X86.HasAVX512F &&
		cpu.X86.HasAVX512BW &&
		cpu.X86.HasAVX512DQ &&
		cpu.X86.HasAVX512CD {
		return OptimizationLevelAVX512V1
	}

	return OptimizationLevelNone
}

// DetectOptimizationLevel detects the optimization level to use based on
// both CPU and `SNELLER_OPT_LEVEL` environment variable, which is useful
// to override the detection.
func DetectOptimizationLevel() OptimizationLevel {
	val, _ := os.LookupEnv(optimizationLevelEnvVar)
	detected := optimizationLevelFromCPUFeatures()
	envLevel := OptimizationLevelDetect

	val = strings.ToLower(val)

	switch val {
	default:
	case "": // do nothing
		return detected

	case "none", "disabled":
		return OptimizationLevelNone

	case "v1", "avx512_v1":
		envLevel = OptimizationLevelAVX512V1

	case "v2", "avx512_v2":
		envLevel = OptimizationLevelAVX512V2
	}

	if envLevel <= detected {
		return envLevel
	}

	return detected
}

// GetOptimizationLevel returns the optimization level currently in use.
func GetOptimizationLevel() OptimizationLevel {
	return globalOptimizationLevel
}

// SetOptimizationLevel sets SSA instructions to use opcodes from given
// optimization level.
//
// NOTE: This function is not thread safe and can be only used at startup
// time or during testing. Its always called on startup to setup the defaults,
// but some tests can call it to make sure we are testing all possible features.
func SetOptimizationLevel(opt OptimizationLevel) {
	if opt == OptimizationLevelDetect {
		opt = DetectOptimizationLevel()
	}

	switch opt {
	case OptimizationLevelAVX512V1:
		initssadefs()

	case OptimizationLevelAVX512V2:
		patchssadefs(patchAVX512Level2)
	}

	globalOptimizationLevel = opt
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

// isSupported determines whether the provided bytecode op
// is supported on the current hardware
func isSupported(bc bcop) bool {
	// consider creating a case switch in ops_gen.go that
	// given a bc returns the level. Then we can remove the
	// inefficient code below
	if DetectOptimizationLevel() == OptimizationLevelAVX512V2 {
		return true
	}
	for _, repl := range patchAVX512Level2 {
		if repl.to == bc {
			return false
		}
	}
	return true
}
