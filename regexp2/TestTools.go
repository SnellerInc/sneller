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

package regexp2

import (
	"fmt"
	"io"
	"os"
	"regexp"

	"golang.org/x/sys/cpu"
)

type DataStructures struct {
	Expr                string
	RegexGolang         *regexp.Regexp
	RegexSneller        *regexp.Regexp
	RegexSupported      bool
	DsT6, DsT7, DsT8    []byte
	DsT6Z, DsT7Z, DsT8Z []byte
	DsLZ                []byte
}

func createFileWriter(filename string) (io.Writer, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	return io.Writer(f), nil
}

// CreateDs creates data-structures for the provided regex string
func CreateDs(expr string, regexType RegexType, writeDot bool, maxNodes int) (DataStructures, error) {
	tmpPath := os.TempDir() + "\\sneller\\"

	result := DataStructures{}
	result.Expr = expr
	x := GolangRegexp
	if regexType == SimilarTo {
		x = GolangSimilarTo
	}
	var err error
	result.RegexSupported = false
	if result.RegexGolang, err = Compile(expr, x); err != nil {
		return result, fmt.Errorf("%v::CreateDs", err)
	}
	if result.RegexSneller, err = Compile(expr, regexType); err != nil {
		return result, fmt.Errorf("%v::CreateDs", err)
	}
	if IsSupported(expr) != nil {
		return result, err
	}
	result.RegexSupported = true
	store, err := CompileDFADebug(result.RegexSneller, writeDot, maxNodes)
	if err != nil {
		return result, fmt.Errorf("%v::CreateDs", err)
	}

	nNodes := 0
	nGroups := 0
	exprEscaped := PrettyStrForDot(result.RegexGolang.String())
	hasRLZA := store.HasRLZA()
	hasUnicodeEdge := store.HasUnicodeEdge()

	if cpu.X86.HasAVX512VBMI && !hasUnicodeEdge {
		hasWildcard, wildcardGroup := store.HasUnicodeWildcard()
		dsTiny, err := NewDsTiny(store)
		if err != nil {
			return result, err
		}
		nNodes = dsTiny.Store.NumberOfNodes()
		nGroups = dsTiny.NumberOfGroups()
		if ds6, valid, dot := dsTiny.DataWithGraphviz(writeDot, 6, hasWildcard, wildcardGroup); valid {
			if hasRLZA {
				result.DsT6Z = ds6
			} else {
				result.DsT6 = ds6
			}
			if writeDot {
				if hasRLZA {
					dot.WriteToFile(tmpPath+"Tiny6Z.dot", "Tiny6Z", exprEscaped)
				} else {
					dot.WriteToFile(tmpPath+"Tiny6.dot", "Tiny6", exprEscaped)
				}
			}
		}
		if ds7, valid, dot := dsTiny.DataWithGraphviz(writeDot, 7, hasWildcard, wildcardGroup); valid {
			if hasRLZA {
				result.DsT7Z = ds7
			} else {
				result.DsT7 = ds7
			}
			if writeDot {
				if hasRLZA {
					dot.WriteToFile(tmpPath+"Tiny7Z.dot", "Tiny7Z", exprEscaped)
				} else {
					dot.WriteToFile(tmpPath+"Tiny7.dot", "Tiny7", exprEscaped)
				}
			}
		}
		if ds8, valid, dot := dsTiny.DataWithGraphviz(writeDot, 8, hasWildcard, wildcardGroup); valid {
			if hasRLZA {
				result.DsT8Z = ds8
			} else {
				result.DsT8 = ds8
			}
			if writeDot {
				if hasRLZA {
					dot.WriteToFile(tmpPath+"Tiny8Z.dot", "Tiny8Z", exprEscaped)
				} else {
					dot.WriteToFile(tmpPath+"Tiny8.dot", "Tiny8", exprEscaped)
				}
			}
		}
	}

	var dsLZ *DsLarge
	if dsLZ, err = NewDsLarge(store); err == nil {
		result.DsLZ = dsLZ.Data()
	}

	if writeDot { // Dump data-structures to disk
		tmpPath := os.TempDir() + "\\sneller\\"
		if result.DsT6 != nil {
			if writer, err := createFileWriter(tmpPath + "DsT6.txt"); err == nil {
				DumpDebug(writer, result.DsT6, 6, nNodes, nGroups, result.Expr)
			}
		}
		if result.DsT7 != nil {
			if writer, err := createFileWriter(tmpPath + "DsT7.txt"); err == nil {
				DumpDebug(writer, result.DsT7, 7, nNodes, nGroups, result.Expr)
			}
		}
		if result.DsT8 != nil {
			if writer, err := createFileWriter(tmpPath + "DsT8.txt"); err == nil {
				DumpDebug(writer, result.DsT8, 8, nNodes, nGroups, result.Expr)
			}
		}
		if result.DsT6Z != nil {
			if writer, err := createFileWriter(tmpPath + "DsT6Z.txt"); err == nil {
				DumpDebug(writer, result.DsT6Z, 6, nNodes, nGroups, result.Expr)
			}
		}
		if result.DsT7Z != nil {
			if writer, err := createFileWriter(tmpPath + "DsT7Z.txt"); err == nil {
				DumpDebug(writer, result.DsT7Z, 7, nNodes, nGroups, result.Expr)
			}
		}
		if result.DsT8Z != nil {
			if writer, err := createFileWriter(tmpPath + "DsT8Z.txt"); err == nil {
				DumpDebug(writer, result.DsT8Z, 8, nNodes, nGroups, result.Expr)
			}
		}
		if result.DsLZ != nil {
			if writer, err := createFileWriter(tmpPath + "DsLZ.txt"); err == nil {
				dsLZ.DumpDebug(writer)
			}
		}
	}
	return result, nil
}
