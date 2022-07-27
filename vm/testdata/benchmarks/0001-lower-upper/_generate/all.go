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

package main

import (
	"fmt"
	"log"
	"os/exec"
	"strconv"
)

func main() {
	lengths := []struct {
		min  int
		max  int
		name string
	}{
		{
			min:  1,
			max:  8,
			name: "short",
		},
		{
			min:  1,
			max:  32,
			name: "medium",
		},
		{
			min:  1,
			max:  64,
			name: "long",
		},
		{
			min:  32,
			max:  64,
			name: "alllong",
		},
	}

	percentages := []int{0, 5, 25, 50, 75, 100}

	for i, _ := range lengths {
		for _, perc := range percentages {
			path := fmt.Sprintf("upper_%s_%03dperc.bench", lengths[i].name, perc)
			fmt.Printf("Generating %s\n", path)

			cmd := exec.Command("go", "run", "_generate/main.go",
				"-min", strconv.Itoa(lengths[i].min),
				"-max", strconv.Itoa(lengths[i].max),
				"-proc", strconv.Itoa(perc),
				"-out", path)

			err := cmd.Run()
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
