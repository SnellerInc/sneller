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
