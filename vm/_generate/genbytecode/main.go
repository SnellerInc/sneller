// Copyright (C) 2023 Sneller, Inc.
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

package main

import (
	"bytes"
	"crypto/md5"
	"flag"
	"fmt"
	"log"
	"os"
)

var (
	verbose = false
	inpath  string
	gopath  string
	asmpath string
)

func main() {
	flag.StringVar(&inpath, "i", "", "input asm file path")
	flag.StringVar(&gopath, "o", "", "bytecode file path")
	flag.StringVar(&asmpath, "s", "", "assembler file path")
	flag.BoolVar(&verbose, "v", false, "be verbose")
	flag.Parse()
	if gopath == "" || inpath == "" {
		flag.Usage()
		return
	}

	opcodes, err := extractOpcodes(inpath)
	check(err)

	byopcode, err := postprocessOpcodes(opcodes)
	check(err)

	argtype, err := generateArgTypeSeqs(opcodes)
	check(err)

	gofile := bytes.NewBuffer(nil)
	err = writeDefinitions(gofile, opcodes, argtype)
	check(err)

	err = writeOpcodeConstants(gofile, opcodes, byopcode)
	check(err)

	asmfile := bytes.NewBuffer(nil)
	writeAsmTable(asmfile, opcodes)

	checksum := []byte(fmt.Sprintf("// checksum: %x\n", md5.Sum(gofile.Bytes())))
	regenerate := true
	old, err := os.ReadFile(gopath)
	if err == nil {
		regenerate = !bytes.HasSuffix(old, checksum)
	}

	if regenerate {
		gofile.Write(checksum)

		fmt.Printf("Creating %q\n", gopath)
		err = os.WriteFile(gopath, gofile.Bytes(), 0644)
		check(err)

		fmt.Printf("Creating %q\n", asmpath)
		err = os.WriteFile(asmpath, asmfile.Bytes(), 0644)
		check(err)
	}
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func panicf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}
