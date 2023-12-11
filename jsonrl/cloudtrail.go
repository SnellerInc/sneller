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

package jsonrl

import (
	"io"

	"github.com/SnellerInc/sneller/ion"
)

// ConvertCloudtrail works like Convert,
// except that it expects src to be formatted
// like AWS Cloudtrail logs, and it automatically
// flattens the elements of the top-level "Records"
// array into the structure fields.
//
// For example, an input like this:
//
//	{"Records": [{"a": "b"}, {"c": "d"}]}
//
// would become
//
//	{"a": "b"}
//	{"c": "d"}
func ConvertCloudtrail(src io.Reader, dst *ion.Chunker, cons []ion.Field) error {
	st := newState(dst)
	tb := &parser{output: st, constants: cons}
	in := &reader{
		buf:   make([]byte, 0, startObjectSize),
		input: src,
	}
	err := in.lexOne("{")
	if err != nil {
		return err
	}
	err = in.lexOne(`"Records":`)
	if err != nil {
		return err
	}
	err = tb.parseTopLevel(in)
	if err != nil {
		return err
	}
	err = in.lexOne("}")
	if err != nil {
		return err
	}
	// We do *not* flush here.
	// Input files can be small, and flushing
	// forces us to emit a block boundary!
	return nil
}
