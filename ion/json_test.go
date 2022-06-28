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

package ion

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

func TestTicketsToJSON(t *testing.T) {
	buf := testdata(t, "parking.10n")
	dst := bytes.NewBuffer(nil)
	_, err := ToJSON(dst, bufio.NewReader(bytes.NewReader(buf)))
	if err != nil {
		t.Fatal(err)
	}
	firstrow := `{"Ticket": 1103341116, "IssueData": "2000-01-01T00:00:00Z", "IssueTime": 1251, "RPState": "CA", "PlateExpiry": 200304, "Make": "HOND", "BodyStyle": "PA", "Color": "GY", "Location": "13147 WELBY WAY", "Route": "01521", "Agency": 1, "ViolationCode": "4000A1", "ViolationDescr": "NO EVIDENCE OF REG", "Fine": 50, "Latitude": 99999, "Longitude": 99999}`
	s := bufio.NewScanner(dst)
	s.Scan()
	if f := s.Text(); f != firstrow {
		t.Errorf("got  row0 = %q", f)
		t.Errorf("want row0 = %q", firstrow)
	}

	dst.Reset()
	w := NewJSONWriter(dst, '\n')
	n, err := w.Write(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) {
		t.Fatalf("n = %d?", n)
	}
	s = bufio.NewScanner(dst)
	s.Scan()
	if f := s.Text(); f != firstrow {
		t.Errorf("got  row0 = %q", f)
		t.Errorf("want row0 = %q", firstrow)
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestToJSON(t *testing.T) {
	cases := []struct {
		item Datum
		want string
	}{
		{
			item: NewStruct(nil,
				[]Field{
					{
						Label: "blob",
						Value: Blob([]byte{0x0, 0x1, 0x2}),
					},
					{
						Label: "int",
						Value: Int(100),
					},
				},
			),
			want: `{"blob": "AAEC", "int": 100}`,
		},
	}
	contents := func(item Datum) []byte {
		var dst Buffer
		var st Symtab
		item.Encode(&dst, &st)
		tail := dst.Bytes()
		dst.Set(nil)
		st.Marshal(&dst, true)
		return append(dst.Bytes(), tail...)
	}
	for i := range cases {
		mem := contents(cases[i].item)
		want := cases[i].want
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			var dst bytes.Buffer
			_, err := ToJSON(&dst, bufio.NewReader(bytes.NewReader(mem)))
			if err != nil {
				t.Fatal(err)
			}
			got := strings.TrimSpace(dst.String())
			if got != want {
				t.Errorf("got %q", got)
				t.Errorf("want %q", want)
			}

			dst.Reset()
			w := NewJSONWriter(&dst, '\n')
			_, err = w.Write(mem)
			if err != nil {
				t.Fatal(err)
			}
			got = strings.TrimSpace(dst.String())
			if got != want {
				t.Errorf("got %q", got)
				t.Errorf("want %q", want)
			}

		})
	}
}

func TestJSONArray(t *testing.T) {
	st0 := NewStruct(nil,
		[]Field{
			{Label: "Foo", Value: String("foo")},
			{Label: "xyz", Value: UntypedNull{}},
		},
	)
	st1 := NewStruct(nil,
		[]Field{
			{Label: "xyz", Value: String("xyz2")},
			{Label: "abc", Value: Uint(123)},
		},
	)

	var tmp Buffer
	var st Symtab
	var out []byte

	var final bytes.Buffer
	w := NewJSONWriter(&final, ',')

	st0.Encode(&tmp, &st)
	split := tmp.Size()
	st.Marshal(&tmp, true)
	out = append(out, tmp.Bytes()[split:]...)
	out = append(out, tmp.Bytes()[:split]...)

	_, err := w.Write(out)
	if err != nil {
		t.Fatal(err)
	}

	out = out[:0]
	st.Reset()
	tmp.Reset()
	st1.Encode(&tmp, &st)
	split = tmp.Size()
	st.Marshal(&tmp, true)
	out = append(out, tmp.Bytes()[split:]...)
	out = append(out, tmp.Bytes()[:split]...)

	_, err = w.Write(out)
	if err != nil {
		t.Fatal(err)
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	got := final.String()
	if got != `[{"Foo": "foo", "xyz": null},{"xyz": "xyz2", "abc": 123}]` {
		t.Fatal("got", got)
	}
}

func TestTaxiToJSON(t *testing.T) {
	buf := testdata(t, "nyc-taxi.block")
	var dst bytes.Buffer
	w := NewJSONWriter(&dst, '\n')
	_, err := w.Write(buf)
	if err != nil {
		t.Fatal(err)
	}

	// r !cd .. && ./dump testdata/*.block | sed 1p
	wantfirst := `{"VendorID": "VTS", "tpep_pickup_datetime": "2009-01-04T02:52:00Z", "tpep_dropoff_datetime": "2009-01-04T03:02:00Z", "passenger_count": 1, "trip_distance": 2.6299999, "pickup_longitude": -73.99195700000001, "pickup_latitude": 40.721567, "RatecodeID": 0, "store_and_fwd_flag": "", "dropoff_longitude": -73.99380300000001, "dropoff_latitude": 40.69592200000001, "payment_type": "CASH", "fare_amount": 8.9, "surcharge": 0.5, "mta_tax": 0, "tip_amount": 0, "tolls_amount": 0, "total_amount": 9.4}`

	// fields in the newyork taxi dataset
	//lint:ignore U1000 field names have to match JSON names
	//lint:file-ignore ST1003 see above
	type field struct {
		VendorID              string
		tpep_pickup_datetime  time.Time
		tpep_dropoff_datetime time.Time
		passenger_count       int
		trip_distance         float64
		pickup_longitude      float64
		pickup_latitude       float64
		RatecodeID            int
		store_and_fwd_flag    string
		dropoff_longitude     float64
		dropoff_latitude      float64
		payment_type          string
		fare_amount           float32
		surcharge             float32
		mta_tax               float32
		tip_amount            float32
		tolls_amount          float32
		total_amount          float32
	}

	s := bufio.NewScanner(&dst)
	row := 0
	for s.Scan() {
		var f field
		err := json.Unmarshal(s.Bytes(), &f)
		if err != nil {
			t.Fatalf("row %d: %s", row, err)
		}
		if row == 0 && s.Text() != wantfirst {
			t.Errorf("got  row0 = %q", s.Text())
			t.Errorf("want row0 = %q", wantfirst)
		}
		row++
	}
	if row != 8560 {
		t.Errorf("only got %d rows", row)
	}
}

func TestEscapedToJSON(t *testing.T) {
	strs := []string{
		"foo\u0000",
		"\v\tbar",
		"\n\n\r",
	}
	var buf Buffer
	var dst bytes.Buffer
	for i := range strs {
		buf.Reset()
		buf.WriteString(strs[i])
		rd := bytes.NewReader(buf.Bytes())
		dst.Reset()
		_, err := ToJSON(&dst, bufio.NewReader(rd))
		if err != nil {
			t.Fatal(err)
		}
		var out string
		err = json.Unmarshal(dst.Bytes(), &out)
		if err != nil {
			t.Fatal(err)
		}
		if out != strs[i] {
			t.Errorf("%q != %q", out, strs[i])
		}

		// same as above, but w/ JSONWriter
		buf.Reset()
		buf.WriteString(strs[i])
		dst.Reset()
		w := NewJSONWriter(&dst, '\n')
		w.Write(buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		err = json.Unmarshal(dst.Bytes(), &out)
		if err != nil {
			t.Fatal(err)
		}
		if out != strs[i] {
			t.Errorf("%q != %q", out, strs[i])
		}
	}
}

func BenchmarkToJSON(b *testing.B) {
	f, err := os.Open("../testdata/nyc-taxi.block")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	b.ReportAllocs()
	r := bufio.NewReader(f)
	dst := bufio.NewWriter(ioutil.Discard)
	b.SetBytes(1048576)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Seek(0, 0)
		r.Reset(f)
		_, err := ToJSON(dst, r)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONWriter(b *testing.B) {
	buf, err := os.ReadFile("../testdata/nyc-taxi.block")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	w := NewJSONWriter(ioutil.Discard, '\n')
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := w.Write(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}
