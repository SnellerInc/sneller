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

package vm

import (
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
)

func TestDistinct(t *testing.T) {
	// select distinct VendorID, RatecodeID
	on := []expr.Node{
		expr.Ident("VendorID"),
		expr.Ident("RatecodeID"),
	}
	var dst QueryBuffer
	src, err := os.ReadFile("../testdata/nyc-taxi.block")
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
		if dat.IsNull() {
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
		s, err := outrows[i].Struct()
		if err != nil {
			t.Fatalf("row %d is %#v", i, outrows[i])
		}
		vend, ok := s.FieldByName("VendorID")
		if !ok {
			t.Fatalf("row %d missing VendorID", i)
		}
		str, _ := vend.String()
		vendors = append(vendors, str)
	}
	// should produce the unique list of vendors;
	// we know that RatecodeID is always zero, so
	// nothing should change about the output list
	sort.Strings(vendors)
	if !reflect.DeepEqual(vendors, []string{"CMT", "DDS", "VTS"}) {
		t.Errorf("got vendors: %s", vendors)
	}
}
