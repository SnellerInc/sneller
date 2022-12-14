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
	"flag"
	"fmt"
)

const usagePlaceholder = "__usage__"

// the initial flag.CommandLine.Usage value
var flagDefaultUsage func()

func PrintOrderedHelp(order []string) {
	helptext := captureDefaultHelp()

	for _, text := range order {
		msg, ok := helptext[text]
		if ok {
			fmt.Print(msg)
			delete(helptext, text)
		} else {
			fmt.Println("")
			fmt.Println(text)
		}
	}

	if len(helptext) > 0 {
		fmt.Println("")
		fmt.Println("Uncategorized")
		for _, msg := range helptext {
			fmt.Print(msg)
		}
	}
}

func captureDefaultHelp() map[string]string {
	// It's an ugly hack. We depend on the flag implementation:
	// that is, flag calls Write once for each help item.
	var flagcapture flagCapture
	old := flag.CommandLine.Output()
	flag.CommandLine.SetOutput(&flagcapture)
	flagDefaultUsage()
	flag.CommandLine.SetOutput(old)

	// map: cmdname => rendered help text
	helptext := make(map[string]string)
	helptext[usagePlaceholder] = flagcapture.items[0]
	index := 1
	flag.VisitAll(func(f *flag.Flag) {
		msg := flagcapture.items[index]
		helptext[f.Name] = msg
		index += 1
	})

	return helptext
}

// --------------------------------------------------

type flagCapture struct {
	items []string
}

func (fc *flagCapture) Write(p []byte) (int, error) {
	fc.items = append(fc.items, string(p))
	return len(p), nil
}
