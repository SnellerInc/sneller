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

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	generateExhaustiveChecks()
}

type Input struct {
	S string `json:"s"`
	D uint32 `json:"d"`
}

type Output struct {
	L string `json:"l"`
	U string `json:"u"`
	D uint32 `json:"d"`
}

func generateExhaustiveChecks() {
	// Note: character 0x1ef9 is the maximum one that has lower/uppercase
	const maxRune = 0x1ffff
	const count = 10000

	id := 1
	min := rune(0)
	max := min + rune(count)
	for {
		generateExhaustiveCheck(fmt.Sprintf("exhaustive%d-utf8.test", id), min, max)

		id += 1
		min = max
		max += rune(count)
		if max > maxRune {
			break
		}
	}
}

func generateExhaustiveCheck(name string, min, max rune) {
	f, err := os.Create(name)
	checkErr(err)
	defer f.Close()

	const maxPrefix = 16
	const maxSuffix = 16

	ascii := "aBcDeFgHiJkLmNoPqRsTuVwXyZ"
	inputs := make([]Input, 0, 1000)
	prefix := 0
	suffix := 0

	for r := min; r < max; r++ {
		inputs = append(inputs, Input{
			S: ascii[:prefix] + string(r) + ascii[:suffix],
			D: uint32(r),
		})

		prefix += 1
		if prefix > maxPrefix {
			prefix = 0
			suffix += 1
			if suffix > maxPrefix {
				suffix = 0
			}
		}
	}

	writeLn(f, `SELECT d, LOWER(s) AS l, UPPER(s) as u FROM input`)
	writeLn(f, "---")
	for i := range inputs {
		raw, err := json.Marshal(inputs[i])
		checkErr(err)

		writeJson(f, raw)
	}
	writeLn(f, "---")
	var out Output
	for i := range inputs {
		out.D = inputs[i].D
		out.L = strings.ToLower(inputs[i].S)
		out.U = strings.ToUpper(inputs[i].S)
		raw, err := json.Marshal(out)
		checkErr(err)

		writeJson(f, raw)
	}
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func writeLn(f *os.File, s string, args ...any) {
	_, err := fmt.Fprintf(f, s+"\n", args...)
	checkErr(err)
}

func writeJson(f *os.File, b []byte) {
	_, err := f.Write(append(b, '\n'))
	checkErr(err)
}
