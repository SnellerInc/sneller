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
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano()) // needed by evalbc_test.go

	VMDebugLeaksStart()
	v := m.Run()
	leaks := VMDebugLeaksFinish()
	if v == 0 && leaks > 0 {
		f := os.Stdout

		fmt.Fprintf(f, "\n")
		fmt.Fprintf(f, "Memory leaks: %d\n", leaks)
		VMDebugLeaksPrint(f)
		os.Exit(2)
	}
	os.Exit(v)
}
