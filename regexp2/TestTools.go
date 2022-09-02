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

package regexp2

import (
	"io"
	"os"
	"regexp"
	"strings"

	"golang.org/x/sys/cpu"
)

type DataStructures struct {
	Expr                        string
	RegexGolang                 *regexp.Regexp
	RegexSneller                *regexp.Regexp
	RegexSupported              bool
	DsT6, DsT7, DsT8, DsL, DsLZ *[]byte
	DsT6Z, DsT7Z, DsT8Z         *[]byte
}

func createFileWriter(filename string) (io.Writer, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	return io.Writer(f), nil
}

// EscapeNL escapes new line
func EscapeNL(str string) string {
	return strings.ReplaceAll(str, "\n", "\\n")
}

// CreateDs creates data-structures for the provided regex string
func CreateDs(expr string, regexType RegexType, writeDot bool, maxNodes int) DataStructures {
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
		//Internal error when compiling regex
		return result
	}
	if result.RegexSneller, err = Compile(expr, regexType); err != nil {
		//Internal error when compiling regex
		return result
	}
	if IsSupported(expr) != nil {
		return result
	}
	result.RegexSupported = true
	store, err := CompileDFADebug(result.RegexSneller, writeDot, maxNodes)
	if err != nil {
		//Internal error when compiling regex
		return result
	}

	nNodes := 0
	nGroups := 0

	exprEscaped := EscapeNL(result.RegexGolang.String())
	hasRLZA := store.HasRLZA()

	if cpu.X86.HasAVX512VBMI && store.HasOnlyASCII() { // AVX512_VBMI -> Icelake
		if dsTiny, err := NewDsTiny(store); err != nil {
			//Internal error when compiling regex
			return result
		} else {
			nNodes = dsTiny.Store.NumberOfNodes()
			nGroups = dsTiny.NumberOfGroups()
			if ds6, valid, dot := dsTiny.DataWithGraphviz(writeDot, 6, hasRLZA); valid {
				if hasRLZA {
					result.DsT6Z = &ds6
				} else {
					result.DsT6 = &ds6
				}
				if writeDot {
					if hasRLZA {
						dot.WriteToFile(tmpPath+"Tiny6Z.dot", "Tiny6Z", exprEscaped)
					} else {
						dot.WriteToFile(tmpPath+"Tiny6.dot", "Tiny6", exprEscaped)
					}
				}
			}
			if ds7, valid, dot := dsTiny.DataWithGraphviz(writeDot, 7, hasRLZA); valid {
				if hasRLZA {
					result.DsT7Z = &ds7
				} else {
					result.DsT7 = &ds7
				}
				if writeDot {
					if hasRLZA {
						dot.WriteToFile(tmpPath+"Tiny7Z.dot", "Tiny7Z", exprEscaped)
					} else {
						dot.WriteToFile(tmpPath+"Tiny7.dot", "Tiny7", exprEscaped)
					}
				}
			}
			if ds8, valid, dot := dsTiny.DataWithGraphviz(writeDot, 8, hasRLZA); valid {
				if hasRLZA {
					result.DsT8Z = &ds8
				} else {
					result.DsT8 = &ds8
				}
				if writeDot {
					if hasRLZA {
						dot.WriteToFile(tmpPath+"Tiny8Z.dot", "Tiny8Z", exprEscaped)
					} else {
						dot.WriteToFile(tmpPath+"Tiny8.dot", "TinyT8", exprEscaped)
					}
				}
			}
		}
	}

	var dsL, dsLZ *DsLarge
	if hasRLZA {
		if dsLZ, err = NewDsLarge(store, true); err == nil {
			tmp := dsLZ.Data()
			result.DsLZ = &tmp
		}
	} else {
		if dsL, err = NewDsLarge(store, false); err == nil {
			tmp := dsL.Data()
			result.DsL = &tmp
		}
	}

	if writeDot { // Dump data-structures to disk
		tmpPath := os.TempDir() + "\\sneller\\"
		if result.DsT6 != nil {
			if writer, err := createFileWriter(tmpPath + "DsT6.txt"); err == nil {
				DumpDebug(writer, *result.DsT6, 6, nNodes, nGroups, false, store.StartRLZA, result.Expr)
			}
		}
		if result.DsT7 != nil {
			if writer, err := createFileWriter(tmpPath + "DsT7.txt"); err == nil {
				DumpDebug(writer, *result.DsT7, 7, nNodes, nGroups, false, store.StartRLZA, result.Expr)
			}
		}
		if result.DsT8 != nil {
			if writer, err := createFileWriter(tmpPath + "DsT8.txt"); err == nil {
				DumpDebug(writer, *result.DsT8, 8, nNodes, nGroups, false, store.StartRLZA, result.Expr)
			}
		}
		if result.DsT6Z != nil {
			if writer, err := createFileWriter(tmpPath + "DsT6Z.txt"); err == nil {
				DumpDebug(writer, *result.DsT6Z, 6, nNodes, nGroups, true, store.StartRLZA, result.Expr)
			}
		}
		if result.DsT7Z != nil {
			if writer, err := createFileWriter(tmpPath + "DsT7Z.txt"); err == nil {
				DumpDebug(writer, *result.DsT7Z, 7, nNodes, nGroups, true, store.StartRLZA, result.Expr)
			}
		}
		if result.DsT8Z != nil {
			if writer, err := createFileWriter(tmpPath + "DsT8Z.txt"); err == nil {
				DumpDebug(writer, *result.DsT8Z, 8, nNodes, nGroups, true, store.StartRLZA, result.Expr)
			}
		}
		if result.DsL != nil {
			if writer, err := createFileWriter(tmpPath + "DsL.txt"); err == nil {
				dsL.DumpDebug(writer)
			}
		}
		if result.DsLZ != nil {
			if writer, err := createFileWriter(tmpPath + "DsLZ.txt"); err == nil {
				dsLZ.DumpDebug(writer)
			}
		}
	}
	return result
}
