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

package plan

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

func TestOutput(t *testing.T) {
	cases := []struct {
		text string // create temp table
	}{{
		text: "SELECT * INTO foo.bar FROM parking",
	}}
	for i := range cases {
		c := &cases[i]
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			// exec the first query
			q, err := partiql.Parse([]byte(c.text))
			if err != nil {
				t.Fatal(err)
			}
			tmp := t.TempDir()
			env := mkoutenv(t, tmp)
			tree, err := New(q, env)
			if err != nil {
				t.Fatal(err)
				return
			}
			t.Logf("query: %s", expr.ToString(q))
			t.Run("serialize-plan", func(t *testing.T) {
				testPlanSerialize(t, tree)
			})
			t.Logf("plan:\n%s", tree)
			var dst bytes.Buffer
			err = Exec(&ExecParams{
				Plan:   tree,
				Output: &dst,
				Runner: env,
				FS:     env.fs,
			})
			if err != nil {
				t.Fatal(err)
			}
			// get the path from the output
			var st ion.Symtab
			rest, err := st.Unmarshal(dst.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			var tbl string
			_, err = ion.UnpackStruct(&st, rest, func(field string, buf []byte) error {
				var err error
				if field == "table" {
					tbl, _, err = ion.ReadString(buf)
				}
				return err
			})
			if err != nil {
				t.Fatal(err)
			} else if tbl == "" {
				t.Fatal("could not find table in output")
			}
			ep, err := expr.ParsePath(tbl)
			if err != nil {
				t.Fatal(err)
			}
			p, ok := expr.FlatPath(ep)
			if !ok {
				t.Fatalf("%s is not a flat path", expr.ToString(ep))
			}
			// make sure we can open the
			// index file
			idx, err := db.OpenIndex(env.fs, p[0], p[1], env.Key())
			if err != nil {
				t.Fatal(err)
			}
			t.Log("index:", idx)
		})
	}
}

var _ UploadEnv = (*outputenv)(nil)

type outputenv struct {
	testenv
	fs  UploadFS
	key *blockfmt.Key
}

func mkoutenv(t *testing.T, dir string) *outputenv {
	fs := &logfs{t: t, UploadFS: db.NewDirFS(dir)}
	key := new(blockfmt.Key)
	rand.Read(key[:])
	env := &outputenv{fs: fs, key: key}
	env.t = t
	return env
}

func (o *outputenv) Uploader() UploadFS { return o.fs }
func (o *outputenv) Key() *blockfmt.Key { return o.key }

type logfs struct {
	t *testing.T
	UploadFS
}

func (f *logfs) WriteFile(path string, buf []byte) (string, error) {
	etag, err := f.UploadFS.WriteFile(path, buf)
	if err != nil {
		f.t.Logf("writing %q failed: %v", path, err)
		return "", err
	}
	f.t.Logf("writing %q succeeded", path)
	return etag, nil
}
