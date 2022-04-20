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

//go:build go1.18

package jsonrl

import (
	"bytes"
	"io"
	"testing"

	"github.com/SnellerInc/sneller/ion"
)

func FuzzConvert(f *testing.F) {
	objs := []string{
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

		`[ {"x": 2, "y": 3, "z": null}, {"foo": "foo", "bar": "xyzabc123"} ]`,
	}
	for i := range objs {
		f.Add([]byte(objs[i]))
	}
	// confirm no crashes from adversarial input
	f.Fuzz(func(t *testing.T, input []byte) {
		cn := ion.Chunker{W: io.Discard, Align: 2048}
		in := bytes.NewReader(input)
		Convert(in, &cn, nil)
	})
}

func FuzzConvertWithHints(f *testing.F) {
	objs := []struct {
		input string
		hints string
	}{
		{
			input: `{"foo": -300, "bar": 1000, "baz": 3.141, "quux": 3.0, "exp": 3.18e-9, "exp2": 3.1e+1}`,
			hints: ``,
		},
		{
			input: `{"foo": -300, "bar": 1000, "baz": 3.141, "quux": 3.0, "exp": 3.18e-9, "exp2": 3.1e+1}`,
			hints: `{"*": "ignore"}`,
		},
		{
			input: `{"foo": -300, "bar": 1000, "baz": 3.141, "quux": 3.0, "exp": 3.18e-9, "exp2": 3.1e+1}`,
			hints: `{"foo": "int", "quux": "number", "exp2": "number", "*": "ignore"}`,
		},
		{
			input: `{"foo": -300, "bar": { "sub": "test", "sub3": { "inner": "i", "sub2": [1, 2, 3] }, "sub4": "123" }, "baz": 3.141}`,
			hints: `{"foo": "int", "bar.sub": "string", "bar.sub4": "string", "baz": "number", "*": "ignore"}`,
		},
		{
			input: `{"value": "2019-07-26T00:00:00"}`,
			hints: `{"value": "datetime"}`,
		},
		{
			input: `{"value": 1634054285}`,
			hints: `{"value": "unix_seconds"}`,
		},
		{
			input: `{"value": 1337}`,
			hints: `{"value": "string"}`,
		},

		// Test explicit ignore
		{
			input: `{"a": 0, "b": 1, "c": 2}`,
			hints: `{"b": "ignore"}`,
		},

		// Test nesting
		{
			input: `{"a": {"b": { "c": 0 }}, "c": {"b": { "a": 1 }}}`,
			hints: `{"a.*": "int", "*": "ignore"}`,
		},

		// Test "first wins"
		{
			input: `{"a": { "b": { "%": { "d": "0" } }, "%": { "%": { "d": "0" } } }}`,
			hints: `{"a.?.?.d": "int", "a.b.?.d": "string"}`,
		},
		{
			input: `{"a": { "b": { "%": { "d": "0" } }, "%": { "%": { "d": "0" } } }}`,
			hints: `{"a.b.?.d": "string", "a.?.?.d": "int"}`,
		},

		// Test wildcards
		{
			input: `{"a": "0", "%": "1"}`,
			hints: `{"a": "string", "?": "int"}`,
		},
		{
			input: `{"a": "0", "%": { "%": "1" }}`,
			hints: `{"a": "string", "?": "int"}`,
		},
		{
			input: `{"a": "0", "%": { "%": "1" }}`,
			hints: `{"a": "string", "*": "int"}`,
		},

		// Test arrays
		{
			input: `{"a": "0", "b": ["1", {"c": "2"}], "c": 1}`,
			hints: `{"a": "string", "b.[?]": "int"}`,
		},
		{
			input: `{"a": "0", "b": ["1", {"c": "2"}], "c": 1}`,
			hints: `{"a": "string", "b.[*]": "int"}`,
		},
		{
			input: `{"a": "0", "b": ["1", {"c": "2"}], "c": 1}`,
			hints: `{"a": "string", "b.[?].c": "int"}`,
		},
		{
			input: `{"a": "0", "%": ["1"], "c": "1"}`,
			hints: `{"a": "string", "*": "int"}`,
		},
		{
			input: `{"a": "0", "%": [{ "%": "1" }], "c": "1"}`,
			hints: `{"a": "string", "?.[?].?": "int"}`,
		},
		{
			input: `{"a": "0", "%": [{ "%": "1" }], "c": "1"}`,
			hints: `{"a": "string", "?.[*]": "int"}`,
		},
		{
			input: `{"a": "0", "%": [{ "%": "1" }], "c": "1"}`,
			hints: `{"a": "string", "*": "int"}`,
		},
		{
			input: `{"a": [{ "%": "1" }], "b": [{ "%": "2" }]}`,
			hints: `{"a.[*]": "int", "*": "ignore"}`,
		},
	}

	for i := range objs {
		f.Add([]byte(objs[i].input), []byte(objs[i].hints))
	}
	// confirm no crashes from adversarial input
	f.Fuzz(func(t *testing.T, input []byte, hints []byte) {
		cn := ion.Chunker{W: io.Discard, Align: 2048}
		in := bytes.NewReader(input)
		h, err := ParseHint(hints)
		if err != nil {
			return
		}
		Convert(in, &cn, h)
	})
}
