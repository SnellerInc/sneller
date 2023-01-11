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
	"io"
	"io/fs"
	"os"

	"github.com/SnellerInc/sneller/ion/blockfmt"
)

func openarg(rootfs fs.FS, name string) (packed, *blockfmt.Trailer) {
	file, err := rootfs.Open(name)
	if err != nil {
		exitf("opening arg: %s", err)
	}
	f, ok := file.(packed)
	if !ok {
		exitf("%T doesn't implement io.ReaderAt; can't read trailer", file)
	}
	info, err := file.Stat()
	if err != nil {
		f.Close()
		exitf("%s", err)
	}
	trailer, err := blockfmt.ReadTrailer(f, info.Size())
	if err != nil {
		f.Close()
		exitf("reading trailer from %s: %s", name, err)
	}
	return f, trailer
}

func unpack(args []string) {
	var out io.WriteCloser
	var dasho string

	flags := flag.NewFlagSet(args[0], flag.ExitOnError)
	flags.StringVar(&dasho, "o", "-", "output file (\"-\" means stdout)")
	flags.Parse(args[1:])
	args = flags.Args()

	var err error
	if dasho == "-" {
		out = os.Stdout
	} else {
		out, err = os.Create(dasho)
		if err != nil {
			exitf("creating output: %s", err)
		}
	}
	rootfs := root(creds())
	defer out.Close()
	var d blockfmt.Decoder
	for i := range args {
		src, trailer := openarg(rootfs, args[i])
		d.Set(trailer, len(trailer.Blocks))
		_, err := d.Copy(out, io.LimitReader(src, trailer.Offset))
		if err != nil {
			exitf("blockfmt.Decoder.Copy: %s", err)
		}
	}
}

func init() {
	addApplet(applet{
		name: "unpack",
		help: "[-o output] <file> ...",
		desc: `unpack 1 or more packfiles into ion
The command
  $ sdb unpack [-o output] <file> ...
unpacks each of the listed files (from the root specified by -root)
and outputs the decompressed ion data from within the file.

If the -o <output> flag is set, then the output of this
command will be directed to that file.
Otherwise, the output is written to stdout.

See the "fetch" command for downloading files
from the tenant rootfs to the local filesystem.
`,
		run: func(args []string) bool {
			if len(args) < 2 {
				return false
			}
			unpack(args)
			return true
		},
	})
}
