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
//   {"Records": [{"a": "b"}, {"c": "d"}]}
//
// would become
//
//   {"a": "b"}
//   {"c": "d"}
//
func ConvertCloudtrail(src io.Reader, dst *ion.Chunker) error {
	st := newState(dst)
	tb := &parser{output: st}
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
