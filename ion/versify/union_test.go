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
