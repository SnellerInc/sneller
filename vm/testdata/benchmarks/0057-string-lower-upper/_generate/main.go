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
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

type Parameters struct {
	minLength int
	maxLength int
	rows      int
	density   float64
	path      string
}

func main() {
	par := parseFlags()

	rand.Seed(42)
	generate(par)
}

func parseFlags() *Parameters {
	var minLength int64
	var maxLength int64
	var rows int64
	p := Parameters{density: -1}
	flag.Int64Var(&minLength, "min", 1, "minimum length of rows")
	flag.Int64Var(&maxLength, "max", 32, "maximum length of rows")
	flag.Int64Var(&rows, "rows", 1000, "total number of rows")
	flag.StringVar(&p.path, "out", "", "output path")
	flag.Func("proc", "how many % of chars have to UTF-8", func(s string) error {
		var err error
		p.density, err = strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}

		if p.density < 0 || p.density > 100 {
			return fmt.Errorf("density must be in range [0..100]")
		}

		p.density /= 100.0

		return nil
	})

	flag.Parse()
	if p.density < 0 || p.path == "" {
		flag.Usage()
		os.Exit(1)
	}

	p.minLength = int(minLength)
	p.maxLength = int(maxLength)
	p.rows = int(rows)

	return &p
}

type Input struct {
	S string `json:"s"`
}

func generate(p *Parameters) {
	// 1. generate lengths
	lengths := make([]int, p.rows)
	totalLength := 0
	d := p.maxLength - p.minLength + 1
	for i := 0; i < p.rows; i++ {
		n := rand.Intn(d) + p.minLength
		lengths[i] = n
		totalLength += n
	}

	utf8Length := int(float64(totalLength) * p.density)
	asciiLength := totalLength - utf8Length

	// 2. get UTF-8 chars
	utf8 := utf8runes()
	shuffle(utf8)

	src := make([]rune, 0, totalLength)

	for utf8Length > 0 {
		if utf8Length >= len(utf8) {
			src = append(src, utf8...)
			utf8Length -= len(utf8)
		} else {
			src = append(src, utf8[:utf8Length]...)
			utf8Length = 0
		}
	}

	// 3. join UTF-8 and ASCII
	src = append(src, asciirunes(asciiLength)...)
	shuffle(src)

	// 4. write the file
	f, err := os.Create(p.path)
	checkErr(err)
	defer f.Close()

	writeLn(f, `SELECT UPPER(s) as u FROM input`)
	writeLn(f, "---")
	var input Input
	for _, n := range lengths {
		input.S = string(src[:n])
		src = src[n:]

		raw, err := json.Marshal(input)
		checkErr(err)

		writeJSON(f, raw)
	}
}

// utf8runes return a list of runes that have a lower or upper version
func utf8runes() []rune {
	res := make([]rune, 0, 3200)
	for r := rune(0); r < rune(0x1ffff); r++ {
		s := string(r)
		if s != strings.ToLower(s) || s != strings.ToUpper(s) {
			res = append(res, r)
		}
	}

	return res
}

func asciirunes(n int) []rune {
	res := make([]rune, n)

	const lo = 32
	const hi = 127
	const d = hi - lo + 1
	for i := range res {
		res[i] = rune(rand.Intn(d) + lo)
	}

	return res
}

func shuffle(r []rune) {
	rand.Shuffle(len(r), func(i, j int) {
		r[i], r[j] = r[j], r[i]
	})
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

func writeJSON(f *os.File, b []byte) {
	_, err := f.Write(append(b, '\n'))
	checkErr(err)
}
