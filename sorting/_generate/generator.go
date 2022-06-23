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

//go:generate go run generator.go
//go:generate gofmt -w ..

package main

import (
	"log"
	"os"
	"path"
	"text/template"
)

// all parameters for templates
type Parameters struct {
	Suffix     string // function name suffix (consists data type and sort order asc/desc)
	TypeSuffix string // function suffix (consists only data type)
	// for AVX512 counting_sort and isSorted
	CmpOp       string // Go operator
	CmpConstant string // Vcmp constant (from ../../bc_imm_amd64.h)
	// for AVX512 partition
	CmpGreaterEq string
	CmpLessEq    string
	// for scalar partition (.go)
	CmpLess    string
	CmpGreater string

	IndexType     string // Go type name
	IndexSize     int    // size in bytes
	IndexElements int    // index elements in an AVX512 register (64/IndexSize)
	KeyType       string // Go type name
	KeySize       int    // size in bytes
	KeyElements   int    // key elements in an AVX512 register (64/KeySize)

	// asm instructions
	VbroadcastMem string
	Vcmp          string
	Vcompress     string
	Vexpand       string
	LoadKey       string
	StoreKey      string
	LoadIdx       string
	StoreIdx      string
	VecLoadKey    string
	VecLoadIdx    string
	VecStoreKey   string
	VecStoreIdx   string
}

// options required to differ asc/desc sorting procedures
type Procedure struct {
	Suffix     string
	TypeSuffix string
	// for AVX512 counting_sort and isSorted
	CmpOp       string
	CmpConstant string
	// for AVX512 partition
	CmpGreaterEq string
	CmpLessEq    string
	// for scalar partition (.go)
	CmpLess    string
	CmpGreater string
}

type Mapping struct {
	inFile  string
	outFile string
	params  []Parameters // array, as we need to generate asc & desc variants for each procedure
}

func main() {
	generateCode()
	generateBenchmarks()
}

func generateCode() {
	mapping := []Mapping{
		{
			inFile:  "is_sorted.go.in",
			outFile: "../is_sorted.go",
			params: []Parameters{
				merge(Procedure{Suffix: "AscFloat64", CmpOp: "<"}, float64parameters),
				merge(Procedure{Suffix: "DescFloat64", CmpOp: ">"}, float64parameters),
				merge(Procedure{Suffix: "AscUint64", CmpOp: "<"}, uint64parameters),
				merge(Procedure{Suffix: "DescUint64", CmpOp: ">"}, uint64parameters),
			},
		},
		{
			inFile:  "counting_sort.go.in",
			outFile: "../counting_sort.go",
			params: []Parameters{
				merge(Procedure{Suffix: "AscFloat64", CmpConstant: "VCMP_IMM_LT_OQ"}, float64parameters),
				merge(Procedure{Suffix: "DescFloat64", CmpConstant: "VCMP_IMM_GT_OQ"}, float64parameters),
				merge(Procedure{Suffix: "AscUint64", CmpConstant: "VPCMP_IMM_LT"}, uint64parameters),
				merge(Procedure{Suffix: "DescUint64", CmpConstant: "VPCMP_IMM_GT"}, uint64parameters),
			},
		},
		{
			inFile:  "counting_sort.s.in",
			outFile: "../counting_sort.s",
			params: []Parameters{
				merge(Procedure{Suffix: "AscFloat64", CmpConstant: "VCMP_IMM_LT_OQ"}, float64parameters),
				merge(Procedure{Suffix: "DescFloat64", CmpConstant: "VCMP_IMM_GT_OQ"}, float64parameters),
				merge(Procedure{Suffix: "AscUint64", CmpConstant: "VPCMP_IMM_LT"}, uint64parameters),
				merge(Procedure{Suffix: "DescUint64", CmpConstant: "VPCMP_IMM_GT"}, uint64parameters),
			},
		},
		{
			inFile:  "partition.go.in",
			outFile: "../partition.go",
			params: []Parameters{
				merge(Procedure{Suffix: "AscFloat64"}, float64parameters),
				merge(Procedure{Suffix: "DescFloat64"}, float64parameters),
				merge(Procedure{Suffix: "AscUint64"}, uint64parameters),
				merge(Procedure{Suffix: "DescUint64"}, uint64parameters),
			},
		},
		{
			inFile:  "partition.s.in",
			outFile: "../partition.s",
			params: []Parameters{
				merge(Procedure{Suffix: "AscFloat64",
					CmpGreaterEq: "VCMP_IMM_GE_OQ",
					CmpLessEq:    "VCMP_IMM_LE_OQ"}, float64parameters),
				merge(Procedure{Suffix: "DescFloat64",
					CmpGreaterEq: "VCMP_IMM_LE_OQ",
					CmpLessEq:    "VCMP_IMM_GE_OQ"}, float64parameters),
				merge(Procedure{Suffix: "AscUint64",
					CmpGreaterEq: "VPCMP_IMM_GE",
					CmpLessEq:    "VPCMP_IMM_LE"}, uint64parameters),
				merge(Procedure{Suffix: "DescUint64",
					CmpGreaterEq: "VPCMP_IMM_LE",
					CmpLessEq:    "VPCMP_IMM_GE"}, uint64parameters),
			},
		},
		{
			inFile:  "quicksort.go.in",
			outFile: "../quicksort.go",
			params: []Parameters{
				merge(Procedure{TypeSuffix: "Float64"}, float64parameters),
				merge(Procedure{TypeSuffix: "Uint64"}, uint64parameters),
			},
		},
		{
			inFile:  "quicksort_impl.go.in",
			outFile: "../quicksort_impl.go",
			params: []Parameters{
				merge(Procedure{Suffix: "AscFloat64", TypeSuffix: "Float64",
					CmpLess:    "<",
					CmpGreater: ">"}, float64parameters),
				merge(Procedure{Suffix: "DescFloat64", TypeSuffix: "Float64",
					CmpLess:    ">",
					CmpGreater: "<"}, float64parameters),
				merge(Procedure{Suffix: "AscUint64", TypeSuffix: "Uint64",
					CmpLess:    "<",
					CmpGreater: ">"}, uint64parameters),
				merge(Procedure{Suffix: "DescUint64", TypeSuffix: "Uint64",
					CmpLess:    ">",
					CmpGreater: "<"}, uint64parameters),
			},
		},
		{
			inFile:  "avx512_quicksort.go.in",
			outFile: "../avx512_quicksort.go",
			params: []Parameters{
				merge(Procedure{TypeSuffix: "Float64"}, float64parameters),
				merge(Procedure{TypeSuffix: "Uint64"}, uint64parameters),
			},
		},
		{
			inFile:  "avx512_quicksort_impl.go.in",
			outFile: "../avx512_quicksort_impl.go",
			params: []Parameters{
				merge(Procedure{Suffix: "AscFloat64", TypeSuffix: "Float64",
					CmpLess:    "<",
					CmpGreater: ">"}, float64parameters),
				merge(Procedure{Suffix: "DescFloat64", TypeSuffix: "Float64",
					CmpLess:    ">",
					CmpGreater: "<"}, float64parameters),
				merge(Procedure{Suffix: "AscUint64", TypeSuffix: "Uint64",
					CmpLess:    "<",
					CmpGreater: ">"}, uint64parameters),
				merge(Procedure{Suffix: "DescUint64", TypeSuffix: "Uint64",
					CmpLess:    ">",
					CmpGreater: "<"}, uint64parameters),
			},
		},
	}

	funcMap := template.FuncMap{
		"sub": func(a, b int) int { return a - b },
		"mul": func(a, b int) int { return a * b },
	}

	for i := range mapping {
		tmpl, err := template.New(path.Base(mapping[i].inFile)).Funcs(funcMap).ParseFiles(mapping[i].inFile)
		die(err)

		f, err := os.Create(mapping[i].outFile)
		die(err)
		defer f.Close()

		log.Printf("Generating %q", mapping[i].outFile)
		err = tmpl.Execute(f, mapping[i].params)
		die(err)
	}
}

func generateBenchmarks() {
	type dataType struct {
		Suffix    string
		KeyType   string
		KeySize   int
		IndexType string
	}

	type Parameters struct {
		DataTypes  []dataType
		InputSizes []int
		InputTypes []string
	}

	var params Parameters
	params.InputSizes = []int{
		100_000,
		1_000_000,
		10_000_000,
	}

	params.InputTypes = []string{
		"Random",
		"Random1to10",
		"Random1to100",
		"AscendingMaxInt",
		"Ascending1to10",
		"Ascending1to100",
		"DescendingMaxInt",
		"Descending1to10",
		"Descending1to100",
	}

	params.DataTypes = []dataType{
		dataType{Suffix: "Float64",
			KeyType:   "float64",
			KeySize:   8,
			IndexType: "uint64"},
		dataType{Suffix: "Uint64",
			KeyType:   "uint64",
			KeySize:   8,
			IndexType: "uint64"},
	}

	input := "quicksort_test.go.in"
	output := "../quicksort_test.go"
	tmpl, err := template.ParseFiles(input)

	die(err)
	f, err := os.Create(output)
	die(err)
	defer f.Close()
	log.Printf("Generating %q", output)
	err = tmpl.Execute(f, params)
	die(err)
}

func merge(proc Procedure, params Parameters) Parameters {
	params.Suffix = proc.Suffix
	params.TypeSuffix = proc.TypeSuffix
	params.CmpOp = proc.CmpOp
	params.CmpConstant = proc.CmpConstant
	params.CmpGreaterEq = proc.CmpGreaterEq
	params.CmpLessEq = proc.CmpLessEq
	params.CmpGreater = proc.CmpGreater
	params.CmpLess = proc.CmpLess

	return params
}

var float64parameters Parameters = Parameters{
	IndexType:     "uint64",
	IndexSize:     8,
	IndexElements: 64 / 8,
	KeyType:       "float64",
	KeySize:       8,
	KeyElements:   64 / 8,

	VbroadcastMem: "VBROADCASTSD",
	Vcmp:          "VCMPPD",
	Vcompress:     "VCOMPRESSPD",
	Vexpand:       "VEXPANDPD",
	LoadKey:       "MOVQ",
	StoreKey:      "MOVQ",
	LoadIdx:       "MOVQ",
	StoreIdx:      "MOVQ",
	VecLoadKey:    "VMOVDQU64",
	VecLoadIdx:    "VMOVDQU64",
	VecStoreKey:   "VMOVDQU64",
	VecStoreIdx:   "VMOVDQU64",
}

var uint64parameters Parameters = Parameters{
	IndexType:     "uint64",
	IndexSize:     8,
	IndexElements: 64 / 8,
	KeyType:       "uint64",
	KeySize:       8,
	KeyElements:   64 / 8,

	VbroadcastMem: "VPBROADCASTQ",
	Vcmp:          "VPCMPUQ",
	Vcompress:     "VPCOMPRESSQ",
	Vexpand:       "VPEXPANDQ",
	LoadKey:       "MOVQ",
	StoreKey:      "MOVQ",
	LoadIdx:       "MOVQ",
	StoreIdx:      "MOVQ",
	VecLoadKey:    "VMOVDQU64",
	VecLoadIdx:    "VMOVDQU64",
	VecStoreKey:   "VMOVDQU64",
	VecStoreIdx:   "VMOVDQU64",
}

func die(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
