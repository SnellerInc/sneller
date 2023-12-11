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

//go:build go1.18

package partiql

import (
	"testing"
)

func FuzzParse(f *testing.F) {
	// test that we can't cause the parser
	// to panic if we pass it garbage text
	for i := range sameq {
		f.Add([]byte(sameq[i]))
	}
	f.Add([]byte("SELECT \"*\"\"\x05\""))
	f.Fuzz(func(t *testing.T, text []byte) {
		Parse(text)
	})
}
