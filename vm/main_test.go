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
	"bytes"
	"fmt"
	"os"
	"testing"
)

const (
	envvar = "SNELLER_AVX512_LEVEL"
)

func TestMain(m *testing.M) {
	{
		val, _ := os.LookupEnv(envvar)
		switch val {
		default:
			fmt.Printf("Environment variable %q: unknown value %q\n", envvar, val)
			os.Exit(2)

		case "": // do nothing

		case "v1":
			setavx512level(avx512level1)

		case "v2":
			if avx512level() >= avx512level2 {
				setavx512level(avx512level2)
			} else {
				fmt.Printf("Environment variable %q: CPU does not support features required by v2\n", envvar)
				os.Exit(2)
			}
		}
	}

	var leakbuf bytes.Buffer
	ret := 0
	LeakCheck(&leakbuf, func() {
		ret = m.Run()
	})
	if ret == 0 && leakbuf.Len() > 0 {
		ret = 2
		fmt.Println("memory leaks:")
		os.Stdout.Write(leakbuf.Bytes())
	}
	os.Exit(ret)
}
