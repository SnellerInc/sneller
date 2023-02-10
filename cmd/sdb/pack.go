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
	"io/fs"
	"strings"

	"github.com/SnellerInc/sneller/ion/blockfmt"
)

func openinput(rootfs fs.FS, name, format string) blockfmt.Input {
	f, err := rootfs.Open(name)
	if err != nil {
		exitf("opening arg: %s", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		exitf("stat %s: %s", name, err)
	}
	var rf blockfmt.RowFormat
	if format != "" {
		rf = blockfmt.MustSuffixToFormat(format)
	} else {
		for suff, cons := range blockfmt.SuffixToFormat {
			if strings.HasSuffix(name, suff) {
				rf, _ = cons(nil)
				break
			}
		}
		if rf == nil {
			exitf("couldn't infer format of %s", name)
		}
	}
	return blockfmt.Input{
		Path: name,
		ETag: "", // don't care
		Size: info.Size(),
		R:    f,
		F:    rf,
	}
}

func pack(args []string) {
	var (
		dasho string
		dashf string
		dashc string
	)
	flags := flag.NewFlagSet(args[0], flag.ExitOnError)
	flags.StringVar(&dashf, "f", "", "input file format (if empty, automatically inferred from file suffix)")
	flags.StringVar(&dasho, "o", "", "output file")
	flags.StringVar(&dashc, "c", "zion", "compression format (zion, zstd)")
	flags.Parse(args[1:])
	args = flags.Args()
	if dasho == "" {
		exitf("pack requires the -o argument to be present")
	}
	rootfs := root(creds())

	ufs, ok := rootfs.(blockfmt.UploadFS)
	if !ok {
		exitf("rootfs %T does not support uploading", rootfs)
	}

	var inputs []blockfmt.Input
	for i := range args {
		inputs = append(inputs, openinput(rootfs, args[i], dashf))
	}

	up, err := ufs.Create(dasho)
	if err != nil {
		exitf("opening %s for writing: %s", dasho, err)
	}

	c := blockfmt.Converter{
		Inputs:     inputs,
		Output:     up,
		Comp:       dashc,
		Align:      1024 * 1024,
		FlushMeta:  50 * 1024 * 1024,
		TargetSize: 8 * 1024 * 1024,
	}

	err = c.Run()
	if err != nil {
		exitf("conversion: %s", err)
	}
}

func init() {
	addApplet(applet{
		name: "pack",
		help: "[-o output] [-f format] <file> ...",
		desc: `pack 1 or more files into an output file`,
		run: func(args []string) bool {
			if len(args) < 2 {
				return false
			}
			pack(args)
			return true
		},
	})
}
