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

package elastic_proxy

import "testing"

func TestParser(t *testing.T) {
	type testData struct {
		input, output string
	}

	tests := []testData{
		{`foo`, `("$source"."default" ~ '(^|[ \t])(?i)foo([ \t]|$)')`},
		{`test +foo -abc`, `(("$source"."default" ~ '(^|[ \t])(?i)foo([ \t]|$)') AND (NOT ("$source"."default" ~ '(^|[ \t])(?i)abc([ \t]|$)')))`},
		{`foo~`, `("$source"."default" ~ '(^|[ \t])(?i)foo([ \t]|$)')`},
		{`foo~12`, `("$source"."default" ~ '(^|[ \t])(?i)foo([ \t]|$)')`},
		{`foo^32`, `("$source"."default" ~ '(^|[ \t])(?i)foo([ \t]|$)')`},
		{`test AND foo`, `(("$source"."default" ~ '(^|[ \t])(?i)test([ \t]|$)') AND ("$source"."default" ~ '(^|[ \t])(?i)foo([ \t]|$)'))`},
		{`test OR foo`, `(("$source"."default" ~ '(^|[ \t])(?i)test([ \t]|$)') OR ("$source"."default" ~ '(^|[ \t])(?i)foo([ \t]|$)'))`},
		{`test AND (foo OR bar)`, `(("$source"."default" ~ '(^|[ \t])(?i)test([ \t]|$)') AND (("$source"."default" ~ '(^|[ \t])(?i)foo([ \t]|$)') OR ("$source"."default" ~ '(^|[ \t])(?i)bar([ \t]|$)')))`},
		{`_exists_:foo`, `("$source"."foo" IS NOT MISSING)`},
		{`u_boolean_allowed:true`, `("$source"."u_boolean_allowed" = TRUE)`},
		{`u_boolean_allowed:false`, `("$source"."u_boolean_allowed" = FALSE)`},
		{`u_string_path.raw:/search/ path`, `(("$source"."u_string_path" ~ '^(?i)search$') OR ("$source"."default" ~ '(^|[ \t])(?i)path([ \t]|$)'))`},
		{`u_string_path:/search/ path`, `((LOWER("$source"."u_string_path") ~ '(^|[ \t])search([ \t]|$)') OR ("$source"."default" ~ '(^|[ \t])(?i)path([ \t]|$)'))`},
		{`f*o`, `("$source"."default" ~ '(^|[ \t])(?i)f.*o([ \t]|$)')`},
		{`f?o`, `("$source"."default" ~ '(^|[ \t])(?i)f.o([ \t]|$)')`},
		{`foo~`, `("$source"."default" ~ '(^|[ \t])(?i)foo([ \t]|$)')`},
		{`u_integer_count:[1 TO 5}`, `(("$source"."u_integer_count" >= 1) AND ("$source"."u_integer_count" < 5))`},
		{`test:>=10`, `("$source"."test" >= 10)`},
		{`<10`, `("$source"."default" < 10)`},
		{`age:(+>=10 +<20)`, `(("$source"."age" >= 10) AND ("$source"."age" < 20))`},
		{`age:(abc def)^3`, `(("$source"."age" ~ '(^|[ \t])(?i)abc([ \t]|$)') OR ("$source"."age" ~ '(^|[ \t])(?i)def([ \t]|$)'))`},
		{`timestamp:["2019-07-24T01:02:03-07:00" TO "2019-07-25T04:05:06-07:00"}`, "((\"$source\".\"timestamp\" >= `2019-07-24T08:02:03Z`) AND (\"$source\".\"timestamp\" < `2019-07-25T11:05:06Z`))"},
		{`test AND u_boolean_allow:true AND u_string_reason:foo`, `((("$source"."default" ~ '(^|[ \t])(?i)test([ \t]|$)') AND ("$source"."u_boolean_allow" = TRUE)) AND ("$source"."u_string_reason" ~ '(^|[ \t])(?i)foo([ \t]|$)'))`},
		{`u_string_name:/joh?n(ath[oa]n)/`, `(LOWER("$source"."u_string_name") ~ '(^|[ \t])joh?n(ath[oa]n)([ \t]|$)')`},
		{`9166ddf7-10b5-42fb-91cf-198eb4d62a3f`, `("$source"."default" ~ '(^|[ \t])(?i)9166ddf7-10b5-42fb-91cf-198eb4d62a3f([ \t]|$)')`},
	}

	//yyErrorVerbose = true
	//yyDebug = 3

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {

			lex := newQueryStringLexer([]byte(test.input))
			lex.defaultOperator = "OR"
			e := yyParse(lex)
			if e != 0 {
				t.Fatalf("error parsing %q", test.input)
			}
			qc := QueryContext{
				TypeMapping: map[string]TypeMapping{
					"timestamp": {
						Type: "datetime",
					},
					"u_*": {
						Type: "text",
						Fields: map[string]string{
							"keyword": "keyword",
							"raw":     "keyword-ignore-case",
						},
					},
				},
			}
			expr, err := lex.result.Expression(&qc, newQSFieldName("default"))
			if err != nil {
				t.Fatalf("error getting expression from %q: %v", test.input, err)
			}
			text := PrintExprPretty(expr)
			if text != test.output {
				t.Fatalf("parsing %q returned %s, but expected %s", test.input, text, test.output)
			}
		})

	}
}
