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

package jsonrl

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/ion"
)

func TestParseOK(t *testing.T) {
	t.Parallel()
	objs := []string{
		`{}`,
		`{"foo": -300, "bar": 1000, "baz": 3.141, "quux":3.0, "exp": 3.18e-9, "exp2": 3.1e+1}`,
		`   {"foo": -300, "bar": 1000, "baz": 3.141, "quux":3.0, "exp": 3.18e-9, "exp2": 3.1e+1}  `,
		// test larger/small floating-point numbers
		// that require Eisel-Lemire conversion for full precision
		`{"1": "7.18931911124017e+66", "2": "-1.7976931348623157e308"}`,
		`{"foo": null}`,
		`{"foo": true}`,
		`{"xyz": false}`,
		`{"foo": null, "bar": null}`,
		`{"list": ["a b", false], "list2": []}`,
		`{"struct": {"x": 3}, "struct2": {}}`,
		`{"str": "\r\n\u10af\\\"foo\"\b"}`,
		`{"str": "∮ E⋅da = Q,  n → ∞, ∑ f(i) = ∏ g(i), ∀x∈ℝ: ⌈x⌉ = −⌊−x⌋, α ∧ ¬β = ¬(¬α ∨ β), ℕ ⊆ ℕ₀ ⊂ ℤ ⊂ ℚ ⊂ ℝ ⊂ ℂ, ⊥ < a ≠ b ≡ c ≤ d ≪ ⊤ ⇒ (A ⇔ B), 2H₂ + O₂ ⇌ 2H₂O, R = 4.7 kΩ, ⌀ 200 mm" }`,
		`{"str": "\u001B\\"}`,
		`{"str": "# Issue summary\r\n\r\nEverything works perfect, but when i login its shows a error\r\n![image](https://user-images.githubusercontent.com/52906642/136658198-cd493a24-1deb-48c4-9107-147b6af17930.png)\r\n\r\n\r\n## Expected behavior\r\n\r\nI am able to check the store and version by cmd, but when i login it shows error, after login error i run SHOPIFY logout its works\r\n\r\n\r\n\r\n## Actual behavior\r\n\r\nX An unexpected error occured.\r\n        To \u001B]8;;https://github.com/Shopify/shopify-cli/issues/new\u001B\\submit an issue\u001B]8;;\u001B\\ include the stack trace.\r\n        To print the stack trace, add the environment variable SHOPIFY_CLI_STACKTRACE=1.\r\n\r\n\r\n\r\n\r\n## Steps to reproduce the problem\r\n\r\n\r\n\r\n\r\n## Specifications\r\n\r\n- App type: theme\r\n- Operating System: Windows\r\n- Shell: Powersell\r\n- Ruby version (ruby -v): 2.7\r\n"}`,
		// escaped RuneError
		`{"str": "\ufffd"}`,
		// unescaped RuneError
		"{\"str\": \"\ufffd\"}",
		"{\"str\": \"\uffff\"}",
		// this needs to be re-sorted into
		//   `{"name": 4, "imports": 6, "xyz": 3}`
		`{"xyz": 3, "name": 4, "imports": 6}`,

		// sub-structure with un-ordered fields
		`{"x": 2, "name": {"y": 3, "imports": "hi", "name": 0}, "": "empty"}`,

		// issue #592: interleaved multi-byte sequences
		// w/ escape characters
		`{"x": "∮ E⋅da\" \\ \f \t \n \r \b \/"}`,
		// unprintable ascii and unicode characters
		"{\"x\":\"\x7f \u200b\"}",

		// leading sub-list, followed by un-ordered fields
		`{"x": ["x", "y", "z"], "bar": {"x": 0, "name": 3, "y": 100, "baz": {"x": 0, "name": 3, "y": 100}, "baz": {"x": 0, "name": 3, "y": 100}}}`,

		// test various combinations of empty structural components
		`{"empty": {}, "empty2": [], "empty3": {"x": [], "y": {}}}`,

		// generated cloudtrail log data
		`{"awsRegion":"eu-west-3","eventID":"3c9068ab-b843-42fa-8465-aedc85e05b94","eventName":"ListSchemas","eventSource":"redshift-data.amazonaws.com","eventTime":"2021-10-26T17:50:04Z","eventType":"AwsServiceEvent","eventVersion":"1.08","hostname":"b8d06e82dbca","kinesisPartitionKey":"2","managementEvent":true,"randomseed":0.7728478564170693,"readOnly":true,"sourceIPAddress":{"asn":{"number":265842,"organization_name":"NARDANONE PEDRO FEDERICO SALVADOR"},"geoip":{"city":"Rio Negro","country":"Argentina","country_code":"AR","location":{"lat":-39.0987,"lon":-67.0702}},"ipaddress":"181.80.38.29"},"userAgent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36 Edg/91.0.864.71","userIdentity":{"accountId":"789123456772","invokedBy":"redshift-data.amazonaws.com","principalId":"ABCDEFGHEVSQ6C2RAND77","sessionContext":{"attributes":{"creationDate":"2021-09-15 23:13:23.2335","mfaAuthenticated":true,"sourceIdentity":"123125"}},"type":"FederatedUser"}}`,

		// some rows from parking3.json
		`{"Make":"ACUR","Entries":[{"Ticket":1104820732,"Color":"WH","BodyStyle":"PA"},{"Ticket":1108321701,"Color":"BK","BodyStyle":"PA"},{"Ticket":1109056185,"Color":"GY","BodyStyle":"PA"},{"Ticket":1109704923,"Color":"BK","BodyStyle":"PA"},{"Ticket":1112085752,"Color":"SI","BodyStyle":"PA"},{"Ticket":1112718014,"Color":"SI","BodyStyle":"PA"},{"Ticket":1113964596,"Color":"SI","BodyStyle":"PA"},{"Ticket":1113964666,"Color":"GY","BodyStyle":"PA"},{"Ticket":1113965156,"Color":"GY","BodyStyle":"PA"},{"Ticket":4271040712,"Color":"BK","BodyStyle":"PA"},{"Ticket":4272091633,"Color":"RD","BodyStyle":"PA"},{"Ticket":4272197473,"Color":"GN","BodyStyle":"PA"},{"Ticket":4272253635,"Color":"SL","BodyStyle":"PA"},{"Ticket":4272385390,"Color":"GY","BodyStyle":"PA"},{"Ticket":4272419546,"Color":"SL","BodyStyle":"PA"}]}`,
		`{"Make":"BMW","Entries":[{"Ticket":1106506446,"Color":"BK","BodyStyle":"PA"},{"Ticket":1108311035,"Color":"BK","BodyStyle":"PA"},{"Ticket":1111881573,"Color":"WH","BodyStyle":"SU"},{"Ticket":1112064730,"Color":"BK","BodyStyle":"PA"},{"Ticket":1112069534,"Color":"WH","BodyStyle":"PA"},{"Ticket":1112070411,"Color":"GY","BodyStyle":"PA"},{"Ticket":1112071730,"Color":"SI","BodyStyle":null},{"Ticket":1112078214,"Color":"BK","BodyStyle":"TR"},{"Ticket":1112078225,"Color":"WH","BodyStyle":"PA"},{"Ticket":1112089915,"Color":"WH","BodyStyle":"PA"},{"Ticket":1112096296,"Color":"BK","BodyStyle":"PA"},{"Ticket":1112099015,"Color":"GY","BodyStyle":"PA"},{"Ticket":1112103145,"Color":"SI","BodyStyle":"PA"},{"Ticket":1112103156,"Color":"WH","BodyStyle":"PA"},{"Ticket":1112717480,"Color":"BL","BodyStyle":"PA"},{"Ticket":1112718040,"Color":"BL","BodyStyle":"PA"},{"Ticket":1112718073,"Color":"BK","BodyStyle":"PA"},{"Ticket":1113011502,"Color":"GY","BodyStyle":"PA"},{"Ticket":1113011546,"Color":"SI","BodyStyle":"PA"},{"Ticket":1113011561,"Color":"RE","BodyStyle":"PA"},{"Ticket":1113011583,"Color":"WH","BodyStyle":"PA"},{"Ticket":1113879815,"Color":"GY","BodyStyle":"PA"},{"Ticket":1113965484,"Color":null,"BodyStyle":"PA"},{"Ticket":1113965650,"Color":"GR","BodyStyle":"PA"},{"Ticket":4269730636,"Color":"WT","BodyStyle":"PA"},{"Ticket":4270720473,"Color":"SL","BodyStyle":"PA"},{"Ticket":4271040690,"Color":"GY","BodyStyle":"PA"},{"Ticket":4271419202,"Color":"WT","BodyStyle":"PA"},{"Ticket":4271459824,"Color":"GY","BodyStyle":"PA"},{"Ticket":4271615736,"Color":"WT","BodyStyle":"PA"},{"Ticket":4271891783,"Color":"SL","BodyStyle":"PA"},{"Ticket":4271891794,"Color":"SL","BodyStyle":"PA"},{"Ticket":4271978804,"Color":"BL","BodyStyle":"PA"},{"Ticket":4271978815,"Color":"BL","BodyStyle":"PA"},{"Ticket":4271978826,"Color":"WT","BodyStyle":"PA"},{"Ticket":4272091574,"Color":"MR","BodyStyle":"PA"},{"Ticket":4272091644,"Color":"GY","BodyStyle":"PA"},{"Ticket":4272091666,"Color":"GY","BodyStyle":"PA"},{"Ticket":4272091670,"Color":"GY","BodyStyle":"PA"},{"Ticket":4272155893,"Color":"BK","BodyStyle":"PA"},{"Ticket":4272155926,"Color":"BK","BodyStyle":"PA"},{"Ticket":4272253650,"Color":"WT","BodyStyle":"PA"},{"Ticket":4272253731,"Color":"BK","BodyStyle":"PA"},{"Ticket":4272285080,"Color":"BK","BodyStyle":"PA"},{"Ticket":4272299990,"Color":"WT","BodyStyle":"PA"},{"Ticket":4272301655,"Color":"BK","BodyStyle":"PA"},{"Ticket":4272301692,"Color":"MR","BodyStyle":"PA"},{"Ticket":4272349325,"Color":"BK","BodyStyle":"PA"},{"Ticket":4272349664,"Color":"GY","BodyStyle":"PA"},{"Ticket":4272419605,"Color":"BK","BodyStyle":"PA"}]}`,
	}
	for i := range objs {
		text := objs[i]
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			cn := &ion.Chunker{W: ioutil.Discard, Align: 1024 * 1024}
			st := newState(cn)
			in := &reader{
				buf:   make([]byte, 0, 10),
				input: strings.NewReader(text),
			}
			tb := &parser{output: st}
			err := tb.parseTopLevel(in)
			if err != nil {
				t.Log(text)
				t.Fatal(err)
			}
			got, _, err := ion.ReadDatum(&st.out.Symbols, st.out.Bytes())
			if err != nil {
				t.Fatalf("reading output: %s", err)
			}
			// check against the slow path
			d := json.NewDecoder(strings.NewReader(text))
			dat, err := ion.FromJSON(&st.out.Symbols, d)
			if err != nil {
				t.Fatalf("ion.FromJSON: %s", err)
			}
			if !ion.Equal(dat, got) {
				t.Error("datum not equal")
			}
		})
	}
}

func TestParseFail(t *testing.T) {
	objs := []string{
		"{x}",
		"{{}",
		`{"\\\s":3}`,
		"{\"x\":[}",
		"[[[}}}",
		`"\xfffd"`,
		`{"str": "\xfffd"}`,
		`{"str": "", }`,
		`{,}`,
		// lexer should reject non-struct value
		// in a top-level list
		`[0,{}]`,
		// lexer should reject invalid utf8
		`{"str": "` + string([]byte{0xff, 0xf0}) + `"}`,
	}
	for i := range objs {
		text := objs[i]
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			st := newState(&ion.Chunker{W: ioutil.Discard, Align: 10000})
			_, err := parseObject(st, []byte(text))
			if err == nil {
				t.Fatal("no error?")
			}
		})
	}
}

func TestParseWithHints(t *testing.T) {
	objs := []struct {
		input    string
		hints    string
		expected string
	}{
		{
			input:    `{"foo": -300, "bar": 1000, "baz": 3.141, "quux": 3.0, "exp": 3.18e-9, "exp2": 3.1e+1}`,
			hints:    ``,
			expected: `{"foo": -300, "bar": 1000, "baz": 3.141, "quux": 3, "exp": 3.18e-09, "exp2": 31}`,
		},
		{
			input:    `{"foo": -300, "bar": 1000, "baz": 3.141, "quux": 3.0, "exp": 3.18e-9, "exp2": 3.1e+1}`,
			hints:    `{"*": "ignore"}`,
			expected: `{}`,
		},
		{
			input:    `{"foo": -300, "bar": 1000, "baz": 3.141, "quux": 3.0, "exp": 3.18e-9, "exp2": 3.1e+1}`,
			hints:    `{"foo": "int", "quux": "number", "exp2": "number", "*": "ignore"}`,
			expected: `{"foo": -300, "quux": 3, "exp2": 31}`,
		},
		{
			input:    `{"foo": -300, "bar": { "sub": "test", "sub3": { "inner": "i", "sub2": [1, 2, 3] }, "sub4": "123" }, "baz": 3.141}`,
			hints:    `{"foo": "int", "bar.sub": "string", "bar.sub4": "string", "baz": "number", "*": "ignore"}`,
			expected: `{"foo": -300, "bar": {"sub": "test", "sub4": "123"}, "baz": 3.141}`,
		},
		{
			input:    `{"value": "2019-07-26T00:00:00"}`,
			hints:    `{"value": "datetime"}`,
			expected: `{"value": "2019-07-26T00:00:00Z"}`,
		},
		{
			input:    `{"value": 1634054285}`,
			hints:    `{"value": "unix_seconds"}`,
			expected: `{"value": "2021-10-12T15:58:05Z"}`,
		},
		{
			input:    `{"value": 1337}`,
			hints:    `{"value": "string"}`,
			expected: `{"value": "1337"}`,
		},

		// Test explicit ignore
		{
			input:    `{"a": 0, "b": 1, "c": 2}`,
			hints:    `{"b": "ignore"}`,
			expected: `{"a": 0, "c": 2}`,
		},

		// Test nesting
		{
			input:    `{"a": {"b": { "c": 0 }}, "c": {"b": { "a": 1 }}}`,
			hints:    `{"a.*": "int", "*": "ignore"}`,
			expected: `{"a": {"b": {"c": 0}}}`,
		},

		// Test "first wins"
		{
			input:    `{"a": { "b": { "%": { "d": "0" } }, "%": { "%": { "d": "0" } } }}`,
			hints:    `{"a.?.?.d": "int", "a.b.?.d": "string"}`,
			expected: `{"a": {"b": {"%": {"d": 0}}, "%": {"%": {"d": 0}}}}`,
		},
		{
			input:    `{"a": { "b": { "%": { "d": "0" } }, "%": { "%": { "d": "0" } } }}`,
			hints:    `{"a.b.?.d": "string", "a.?.?.d": "int"}`,
			expected: `{"a": {"b": {"%": {"d": "0"}}, "%": {"%": {"d": 0}}}}`,
		},

		// Test wildcards
		{
			input:    `{"a": "0", "%": "1"}`,
			hints:    `{"a": "string", "?": "int"}`,
			expected: `{"a": "0", "%": 1}`,
		},
		{
			input:    `{"a": "0", "%": { "%": "1" }}`,
			hints:    `{"a": "string", "?": "int"}`,
			expected: `{"a": "0", "%": {"%": "1"}}`,
		},
		{
			input:    `{"a": "0", "%": { "%": "1" }}`,
			hints:    `{"a": "string", "*": "int"}`,
			expected: `{"a": "0", "%": {"%": 1}}`,
		},

		// Test arrays
		{
			input:    `{"a": "0", "b": ["1", {"c": "2"}], "c": 1}`,
			hints:    `{"a": "string", "b.[?]": "int"}`,
			expected: `{"a": "0", "b": [1, {"c": "2"}], "c": 1}`,
		},
		{
			input:    `{"a": "0", "b": ["1", {"c": "2"}], "c": 1}`,
			hints:    `{"a": "string", "b.[*]": "int"}`,
			expected: `{"a": "0", "b": [1, {"c": 2}], "c": 1}`,
		},
		{
			input:    `{"a": "0", "b": ["1", {"c": "2"}], "c": 1}`,
			hints:    `{"a": "string", "b.[?].c": "int"}`,
			expected: `{"a": "0", "b": ["1", {"c": 2}], "c": 1}`,
		},
		{
			input:    `{"a": "0", "%": ["1"], "c": "1"}`,
			hints:    `{"a": "string", "*": "int"}`,
			expected: `{"a": "0", "%": [1], "c": 1}`,
		},
		{
			input:    `{"a": "0", "%": [{ "%": "1" }], "c": "1"}`,
			hints:    `{"a": "string", "?.[?].?": "int"}`,
			expected: `{"a": "0", "%": [{"%": 1}], "c": "1"}`,
		},
		{
			input:    `{"a": "0", "%": [{ "%": "1" }], "c": "1"}`,
			hints:    `{"a": "string", "?.[*]": "int"}`,
			expected: `{"a": "0", "%": [{"%": 1}], "c": "1"}`,
		},
		{
			input:    `{"a": "0", "%": [{ "%": "1" }], "c": "1"}`,
			hints:    `{"a": "string", "*": "int"}`,
			expected: `{"a": "0", "%": [{"%": 1}], "c": 1}`,
		},
		{
			input:    `{"a": [{ "%": "1" }], "b": [{ "%": "2" }]}`,
			hints:    `{"a.[*]": "int", "*": "ignore"}`,
			expected: `{"a": [{"%": 1}]}`,
		},
		{
			input:    `{"a": [{ "%": "1" }], "b": [{ "%": "2" }], "c": [{ "%": "3" }]}`,
			hints:    `{"b": "ignore", "b.*": "int"}`,
			expected: `{"a": [{"%": "1"}], "c": [{"%": "3"}]}`,
		},
	}
	for i := range objs {
		test := objs[i]
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			st := newState(&ion.Chunker{W: ioutil.Discard, Align: 10000})

			if test.hints != "" {
				entry, err := ParseHint([]byte(test.hints))
				if err != nil {
					t.Fatalf("invalid hints: %s", err)
				}
				st.UseHints(entry)
			}

			n, err := parseObject(st, []byte(test.input))
			if err != nil {
				t.Fatalf("position %d: %s", n, err)
			}
			err = st.Commit()
			if err != nil {
				t.Fatal(err)
			}
			g, _, err := ion.ReadDatum(&st.out.Symbols, st.out.Bytes())
			if err != nil {
				t.Fatalf("reading output: %s", err)
			}

			got, err := toJSONString(g, &st.out.Symbols)
			if err != nil {
				t.Fatalf("error converting ion -> string")
			}

			if strings.TrimRight(got, "\n") != test.expected {
				t.Logf("got : %s", got)
				t.Logf("want: %s", test.expected)
				t.Error("json not equal")
			}
		})
	}
}

func timestamp(s string) ion.Timestamp {
	t, ok := date.Parse([]byte(s))
	if !ok {
		panic("bad timestamp: " + s)
	}
	return ion.Timestamp(t)
}

func TestIssue1236(t *testing.T) {
	var dst ion.Chunker
	st := newState(&dst)
	b := reader{
		buf:  []byte{' ', ' ', '\n'},
		rpos: 0,
	}
	tb := &parser{output: st}
	err := tb.lexToplevel(&b)
	if err != nil {
		t.Fatal(err)
	}
}

// This mostly exists to validate that the parser calls
// Write and End methods on the Chunker instead of
// Chunker.Buffer directly, since most of the range
// tracking logic is tested in package ion.
func TestParseRanges(t *testing.T) {
	cases := []struct {
		inputs []string
		ranges []ranges
	}{{
		inputs: []string{`{"foo":"2021-11-10T00:00:00Z"}`},
		ranges: []ranges{{
			path: []string{"foo"},
			min:  timestamp("2021-11-10T00:00:00Z"),
			max:  timestamp("2021-11-10T00:00:00Z"),
		}},
	}, {
		inputs: []string{
			`{"foo":{"date":"2021-11-10T00:00:00Z"},"bar":{"date":"2021-11-20T00:00:00Z"}}`,
			`{"foo":{"date":"2021-11-10T01:00:00Z"},"bar":{"date":"2021-11-20T01:00:00Z"}}`,
			`{"foo":{"date":"2021-11-10T02:00:00Z"},"bar":{"date":"2021-11-20T02:00:00Z"}}`,
			`{"foo":{"date":"2021-11-10T03:00:00Z"},"bar":{"date":"2021-11-20T03:00:00Z"}}`,
		},
		ranges: []ranges{{
			path: []string{"foo", "date"},
			min:  timestamp("2021-11-10T00:00:00Z"),
			max:  timestamp("2021-11-10T03:00:00Z"),
		}, {
			path: []string{"bar", "date"},
			min:  timestamp("2021-11-20T00:00:00Z"),
			max:  timestamp("2021-11-20T03:00:00Z"),
		}},
	}, {
		inputs: []string{`{"foo":"2021-11-10T00:00:00Z","bar":["2021-11-20T02:00:00Z"]}`},
		ranges: []ranges{{
			path: []string{"foo"},
			min:  timestamp("2021-11-10T00:00:00Z"),
			max:  timestamp("2021-11-10T00:00:00Z"),
		}},
	}, {
		inputs: []string{
			`{"foo":{"bar":"2021-11-24T00:00:00Z"}}`,
			`{"foo":{"bar":"2021-11-24T02:00:00Z"}}`,
			`{"foo":[123, {"bar":"2021-11-24T04:00:00Z"}]}`,
		},
		ranges: []ranges{{
			path: []string{"foo", "bar"},
			min:  timestamp("2021-11-24T00:00:00Z"),
			max:  timestamp("2021-11-24T02:00:00Z"),
		}},
	}}
	for i := range cases {
		tc := &cases[i]
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			var rw rangeWriter
			cn := &ion.Chunker{W: &rw, Align: 1024 * 1024}
			st := newState(cn)
			for _, in := range tc.inputs {
				n, err := parseObject(st, []byte(in))
				if err != nil {
					t.Fatalf("position %d: %s", n, err)
				}
				if n != len(in) {
					t.Errorf("parse %d of %d bytes", n, len(in))
				}
				st.out.Commit()
			}
			st.out.Flush()
			if !reflect.DeepEqual(tc.ranges, rw.ranges) {
				t.Errorf("ranges not equal")
				t.Errorf("want: %v", tc.ranges)
				t.Errorf("got:  %v", rw.ranges)
			}
		})
	}
}

type readfn func(p []byte) (int, error)

func (r readfn) Read(p []byte) (int, error) {
	return r(p)
}

func repeat(text string, count int) io.Reader {
	rd := strings.NewReader(text)
	remain := count
	return readfn(func(p []byte) (int, error) {
		if remain == 0 {
			return 0, io.EOF
		}
		n := 0
		for n < len(p) && remain > 0 {
			nn, err := rd.Read(p[n:])
			n += nn
			if err == io.EOF {
				rd.Reset(text)
				remain--
			}
		}
		return n, nil
	})
}

func TestHuge(t *testing.T) {
	t.Parallel()
	// create an object that is much
	// larger than MaxDatumSize but
	// doesn't have any fields that
	// would bother the parser
	m := make(map[string]string)
	str := strings.Repeat("foobarbazquux\n", 1000)
	for i := 0; i < 500; i++ {
		m[fmt.Sprintf("field%d", i)] = fmt.Sprintf("%s%d", str, i)
	}
	buf, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	cn := &ion.Chunker{W: ioutil.Discard, Align: 10000000}
	err = Convert(bytes.NewReader(buf), cn, nil)
	if err != nil {
		t.Fatal(err)
	}
	// an additional 500 fields should max out
	// the alignment, at which point we should
	// get an error
	for i := 0; i < 500; i++ {
		m[fmt.Sprintf("field%d_2", i)] = fmt.Sprintf("%s%d", str, i)
	}
	buf, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	err = Convert(bytes.NewReader(buf), cn, nil)
	if !errors.Is(err, ion.ErrTooLarge) {
		t.Errorf("didn't get ion.ErrTooLarge? got %v", err)
	}

	// test that a struct field that is too large
	// produces the right error *and* context
	cn = &ion.Chunker{W: ioutil.Discard, Align: 1024 * 1024}
	text := io.MultiReader(
		strings.NewReader(`{"x": "`),
		repeat("xy", MaxDatumSize),
		strings.NewReader(`"}`),
	)
	err = Convert(text, cn, nil)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("didn't get ion.ErrTooLarge? got %v", err)
	}
	if !strings.Contains(err.Error(), "field \"x\"") {
		t.Errorf("error doesn't include context: %s", err)
	}
}

// don't allow objects to exceed a reasonable level of nesting
func TestMaxDepth(t *testing.T) {
	t.Parallel()
	text := strings.Repeat("{\"x\":", MaxObjectDepth+1) + strings.Repeat("}", MaxObjectDepth+1)
	cn := &ion.Chunker{W: ioutil.Discard, Align: 1000}
	err := Convert(strings.NewReader(text), cn, nil)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("expected ErrTooLarge, got %v", err)
	}

	// for arrays as well:
	text = "{\"x\":" + strings.Repeat("[", MaxObjectDepth+1) + strings.Repeat("]", MaxObjectDepth+1)
	err = Convert(strings.NewReader(text), cn, nil)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("expected ErrTooLarge, got %v", err)
	}
}

type loopReader struct {
	r     io.ReadSeeker
	count int
}

func (l *loopReader) Read(p []byte) (int, error) {
	for {
		n, err := l.r.Read(p)
		if n > 0 || err != io.EOF || l.count <= 0 {
			return n, err
		}
		_, err = l.r.Seek(0, 0)
		if err != nil {
			return 0, err
		}
		l.count--
	}
}

type counter int64

func (c *counter) Write(p []byte) (int, error) {
	*(*int64)(c) += int64(len(p))
	return len(p), nil
}

func BenchmarkTranslate(b *testing.B) {
	files := []string{
		"parking3.json",
		"parking2.json",
		"cloudtrail.json",
	}

	for i := range files {
		b.Run(files[i], func(b *testing.B) {
			f, err := os.Open("../testdata/" + files[i])
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()
			lp := &loopReader{
				r:     f,
				count: b.N,
			}
			info, err := f.Stat()
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(info.Size())
			b.ReportAllocs()
			w := counter(0)
			cn := &ion.Chunker{
				W:          &w,
				Align:      1024 * 1024,
				RangeAlign: 100 * 1024 * 1024,
			}
			b.ResetTimer()
			err = Convert(lp, cn, nil)
			if err != nil {
				b.Fatal(err)
			}
			b.Logf("%d symbols; %d bytes in; %d bytes out", cn.Symbols.MaxID(), info.Size()*int64(b.N), int64(w))
		})
	}
}

func BenchmarkTranslateWithHints(b *testing.B) {
	objs := []struct {
		input string
		hints string
	}{
		{
			input: `{"0": "a", "1": "a", "2": "a", "3": "a", "4": "a", "5": "a", "6": "a", "7": "a", "8": "a", "9": "a"}`,
			hints: ``,
		},
		{
			input: `{"0": "a", "1": "a", "2": "a", "3": "a", "4": "a", "5": "a", "6": "a", "7": "a", "8": "a", "9": "a"}`,
			hints: `{"0": "string", "1": "string", "2": "string", "3": "string", "4": "string", "5": "string", "6": "string", "7": "string", "8": "string", "9": "string", "*": "ignore"}`,
		},
		{
			input: `{"0": "a", "1": "a", "2": "a", "3": "a", "4": "a", "5": "a", "6": "a", "7": "a", "8": "a", "9": "a"}`,
			hints: `{"0": "string", "1": "string", "2": "string", "3": "string", "4": "string", "*": "ignore"}`,
		},
		{
			input: `{"0": "a", "1": "a", "2": "a", "3": "a", "4": "a", "5": "a", "6": "a", "7": "a", "8": "a", "9": "a"}`,
			hints: `{"5": "string", "6": "string", "7": "string", "8": "string", "9": "string", "*": "ignore"}`,
		},
		{
			input: `{"0": "a", "1": "a", "2": "a", "3": "a", "4": "a", "5": "a", "6": "a", "7": "a", "8": "a", "9": "a"}`,
			hints: `{"0": "string", "2": "string", "4": "string", "6": "string", "8": "string", "*": "ignore"}`,
		},
		{
			input: `{"0": { "inner": "a" }, "1": { "inner": "a" }, "2": { "inner": "a" }, "3": { "inner": "a" }, "4": { "inner": "a" }, "5": { "inner": "a" }, "6": { "inner": "a" }, "7": { "inner": "a" }, "8": { "inner": "a" }, "9": { "inner": "a" }}`,
			hints: ``,
		},
		{
			input: `{"0": { "inner": "a" }, "1": { "inner": "a" }, "2": { "inner": "a" }, "3": { "inner": "a" }, "4": { "inner": "a" }, "5": { "inner": "a" }, "6": { "inner": "a" }, "7": { "inner": "a" }, "8": { "inner": "a" }, "9": { "inner": "a" }}`,
			hints: `{"0.inner": "string", "1.inner": "string", "2.inner": "string", "3.inner": "string", "4.inner": "string", "5.inner": "string", "6.inner": "string", "7.inner": "string", "8.inner": "string", "9.inner": "string", "*": "ignore"}`,
		},
		{
			input: `{"0": { "inner": "a" }, "1": { "inner": "a" }, "2": { "inner": "a" }, "3": { "inner": "a" }, "4": { "inner": "a" }, "5": { "inner": "a" }, "6": { "inner": "a" }, "7": { "inner": "a" }, "8": { "inner": "a" }, "9": { "inner": "a" }}`,
			hints: `{"0.inner": "string", "1.inner": "string", "2.inner": "string", "3.inner": "string", "4.inner": "string", "*": "ignore"}`,
		},
		{
			input: `{"0": { "inner": "a" }, "1": { "inner": "a" }, "2": { "inner": "a" }, "3": { "inner": "a" }, "4": { "inner": "a" }, "5": { "inner": "a" }, "6": { "inner": "a" }, "7": { "inner": "a" }, "8": { "inner": "a" }, "9": { "inner": "a" }}`,
			hints: `{"5.inner": "string", "6.inner": "string", "7.inner": "string", "8.inner": "string", "9.inner": "string", "*": "ignore"}`,
		},
		{
			input: `{"0": { "inner": "a" }, "1": { "inner": "a" }, "2": { "inner": "a" }, "3": { "inner": "a" }, "4": { "inner": "a" }, "5": { "inner": "a" }, "6": { "inner": "a" }, "7": { "inner": "a" }, "8": { "inner": "a" }, "9": { "inner": "a" }}`,
			hints: `{"0.inner": "string", "2.inner": "string", "4.inner": "string", "6.inner": "string", "8.inner": "string", "*": "ignore"}`,
		},
	}
	for i := range objs {
		test := objs[i]
		b.Run(fmt.Sprintf("case-%d", i), func(b *testing.B) {
			buf := []byte(test.input)
			var hints *Hint
			if test.hints != "" {
				entry, err := ParseHint([]byte(test.hints))
				if err != nil {
					b.Fatalf("invalid hints: %s", err)
				}
				hints = entry
			}
			b.SetBytes(int64(len(buf)))
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				cn := &ion.Chunker{W: ioutil.Discard, Align: 1024 * 1024}
				rd := bytes.NewReader(nil)
				for pb.Next() {
					cn.Reset()
					rd.Reset(buf)
					err := Convert(rd, cn, hints)
					if err != nil {
						b.Fatalf("Convert: %s", err)
					}
				}
			})
		})
	}
}

func toJSONString(dat ion.Datum, st *ion.Symtab) (string, error) {

	var datIonBuf ion.Buffer
	st.Marshal(&datIonBuf, true)
	dat.Encode(&datIonBuf, st)

	inByteBuf := bytes.NewBuffer(datIonBuf.Bytes())
	in := bufio.NewReader(inByteBuf)
	out := bytes.NewBuffer(nil)

	_, err := ion.ToJSON(out, in)
	if err != nil {
		return "", err
	}

	return out.String(), nil
}

type ranges struct {
	path     []string
	min, max ion.Datum
}

// rangeWriter is an io.Writer that discards written
// bytes and exposes SetMinMax for range tracking.
type rangeWriter struct {
	ranges []ranges // ranges for current chunk
}

func (w *rangeWriter) SetMinMax(path []string, min, max ion.Datum) {
	w.ranges = append(w.ranges, ranges{
		path: path,
		min:  min,
		max:  max,
	})
}

func (w *rangeWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func TestConvert(t *testing.T) {
	// start with an extremely small
	// buffer to exercise the
	old := startObjectSize
	t.Cleanup(func() { startObjectSize = old })

	// starting buffer sizes to test
	bufsizes := []int{
		4, 7, 15, 42, 128, 256,
	}
	testcases := []struct {
		text     string
		objcount int
	}{
		// no data:
		{``, 0},
		// only whitespace:
		{`	`, 0},
		// leading and trailing whitespace around objects:
		{`

{"foo": 3}{"bar": 4}


`, 2},
		// just trailing whitespace, plus whitespace
		// between objects
		{`{"foo":
3}
{"bar":		4}	`, 2},
	}
	for i := range testcases {
		text := testcases[i].text
		objcount := testcases[i].objcount
		for _, size := range bufsizes {
			startObjectSize = size
			t.Run(fmt.Sprintf("buf=%d/case=%d", size, i), func(t *testing.T) {
				r := strings.NewReader(text)
				var buf bytes.Buffer
				cn := ion.Chunker{
					W:     &buf,
					Align: 1024,
				}
				err := Convert(r, &cn, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := cn.Flush(); err != nil {
					t.Fatal(err)
				}
				if n := count(t, buf.Bytes()); n != objcount {
					t.Errorf("got %d objects out instead of %d", n, objcount)
				}
			})
		}
	}
}

func count(t *testing.T, buf []byte) int {
	var st ion.Symtab
	var dat ion.Datum
	var err error
	n := 0
	for len(buf) > 0 {
		dat, buf, err = ion.ReadDatum(&st, buf)
		if err != nil {
			t.Helper()
			t.Fatal(err)
		}
		t.Logf("datum %#v", dat)
		if dat.Type() == ion.NullType {
			continue // pad
		}
		n++
	}
	return n
}
