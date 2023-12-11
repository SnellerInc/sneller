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
	flags.StringVar(&dashc, "c", "zion+iguana_v0", "compression format (zion, zstd, zion+iguana_v0, ...)")
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
		help: "[-o output] [-f format] [-c compression] <file> ...",
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
