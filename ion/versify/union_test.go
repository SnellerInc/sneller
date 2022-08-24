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

package versify

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

func testFile(t *testing.T, p, descr string) {
	buf, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var st ion.Symtab
	buf, err = st.Unmarshal(buf)
	if err != nil {
		t.Fatal(err)
	}

	var u Union
	var val ion.Datum
	for len(buf) > 0 {
		val, buf, err = ion.ReadDatum(&st, buf)
		if err != nil {
			t.Fatal(err)
		}
		if u == nil {
			u = Single(val)
		} else {
			u = u.Add(val)
		}
	}
	res, ok := u.(*Struct)
	if !ok {
		t.Fatalf("expected structure union; found %T", u)
	}
	descparts := strings.Split(descr, ",\n")
	for i := range descparts {
		descparts[i] = strings.TrimSpace(descparts[i])
	}

	// check that each of descparts corresponds
	// to the structure fields
	for i := range descparts {
		if i >= len(res.fields) {
			t.Fatalf("missing field %d", i)
			break
		}
		got := fmt.Sprintf("%s: %s", res.fields[i], res.values[i])
		if got != descparts[i] {
			t.Errorf("field %d: got  %s", i, got)
			t.Errorf("field %d: want %s", i, descparts[i])
		}
	}
}

func TestDescribe(t *testing.T) {
	tcs := []struct {
		file  string
		descr string
	}{
		{
			"parking.10n",
			`Ticket: integer[1103341116, 4272473892],
IssueData: time[2000-01-01 00:00:00 +0000 UTC, 2000-01-01 00:00:00 +0000 UTC],
IssueTime: any{99.90: integer[18, 2355], 0.10: MISSING},
RPState: string[25 unique],
PlateExpiry: any{93.45: integer[1, 201905], 6.55: MISSING},
Make: any{99.61: string[59 unique], 0.39: MISSING},
BodyStyle: any{99.22: string[10 unique], 0.78: MISSING},
Color: any{99.32: string[24 unique], 0.68: MISSING},
Location: any{99.90: string[824 unique], 0.10: MISSING},
Route: any{97.85: string[138 unique], 2.15: MISSING},
Agency: integer[1, 57],
ViolationCode: string[61 unique],
ViolationDescr: string[58 unique],
Fine: any{98.92: integer[25, 363], 1.08: MISSING},
Latitude: integer[99999, 99999],
Longitude: integer[99999, 99999],
MeterId: any{12.22: string[81 unique], 87.78: MISSING},
MarkedTime: any{0.68: string[3 unique], 99.32: MISSING}
`,
		},
	}
	for i := range tcs {
		t.Run(tcs[i].file, func(t *testing.T) {
			testFile(t, filepath.Join("../../testdata/", tcs[i].file), tcs[i].descr)
		})
	}
}
