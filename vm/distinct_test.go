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

package vm

import (
	"io/ioutil"
	"reflect"
	"sort"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
)

func TestDistinct(t *testing.T) {
	// select distinct VendorID, RatecodeID
	on := []expr.Node{
		&expr.Path{First: "VendorID"},
		&expr.Path{First: "RatecodeID"},
	}
	var dst QueryBuffer
	src, err := ioutil.ReadFile("../testdata/nyc-taxi.block")
	if err != nil {
		t.Fatal(err)
	}
	df, err := NewDistinct(on, &dst)
	if err != nil {
		t.Fatal(err)
	}
	err = CopyRows(df, buftbl(src), 1)
	if err != nil {
		t.Fatal(err)
	}
	result := dst.Bytes()
	var st ion.Symtab
	result, err = st.Unmarshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var outrows []ion.Datum
	for len(result) > 0 {
		var dat ion.Datum
		dat, result, err = ion.ReadDatum(&st, result)
		if err != nil {
			t.Error(err)
			break
		}
		null := ion.UntypedNull{}
		if dat == null {
			break
		}
		outrows = append(outrows, dat)
	}
	if len(outrows) != 3 {
		t.Errorf("%d rows out; expected 3", len(outrows))
	}
	// collect the VendorID fields
	var vendors []string
	for i := range outrows {
		s, ok := outrows[i].(*ion.Struct)
		if !ok {
			t.Fatalf("row %d is %#v", i, outrows[i])
		}
		vend, ok := s.FieldByName("VendorID")
		if !ok {
			t.Fatalf("row %d missing VendorID", i)
		}
		vendors = append(vendors, string(vend.Value.(ion.String)))
	}
	// should produce the unique list of vendors;
	// we know that RatecodeID is always zero, so
	// nothing should change about the output list
	sort.Strings(vendors)
	if !reflect.DeepEqual(vendors, []string{"CMT", "DDS", "VTS"}) {
		t.Errorf("got vendors: %s", vendors)
	}
}
