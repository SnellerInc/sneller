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
	"io"
	"os"
	"path"

	"github.com/SnellerInc/sneller/db"
)

func fetch(creds db.Tenant, files ...string) {
	ofs := root(creds)
	for i := range files {
		if dashv {
			logf("fetching %s...\n", files[i])
		}
		f, err := ofs.Open(files[i])
		if err != nil {
			exitf("%s", err)
		}
		dstf, err := os.Create(path.Base(files[i]))
		if err != nil {
			exitf("creating %s: %s", path.Base(files[i]), err)
		}
		_, err = io.Copy(dstf, f)
		f.Close()
		dstf.Close()
		if err != nil {
			os.Remove(dstf.Name())
			exitf("copying bytes: %s", err)
		}
	}
}

func init() {
	addApplet(applet{
		name: "fetch",
		help: "<file> ...",
		desc: `copy files from the tenant rootfs
The command
  $ sdb fetch <file> ...
fetches the associated file from the rootfs (see -root)
and stores it in the current directory in a file with
the same basename as the respective remote file.

For example,
  $ sdb fetch db/foo/bar/baz.ion.zst
would create a local file called baz.ion.zst.

See also the "unpack" command for unpacking *.ion.zst and *.zion files.
`,
		run: func(args []string) bool {
			if len(args) < 2 {
				return false
			}
			fetch(creds(), args[1:]...)
			return true
		},
	})
}
