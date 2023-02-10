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

package db

import (
	"bytes"
	"fmt"
	"strconv"
	"unicode/utf8"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/fsutil"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"golang.org/x/exp/maps"
)

type partition struct {
	name    string
	prepend int
	cons    []ion.Field
	lst     []blockfmt.Input
}

// A collector is used to collect inputs and
// partition them.
type collector struct {
	def   []Partition
	parts []partition
	ind   map[string]int // index into parts
	buf   []byte
	mr    fsutil.Matcher
}

func (c *collector) total() (n int, size int64) {
	if c == nil {
		return 0, 0
	}
	for i := range c.parts {
		n += len(c.parts[i].lst)
		for j := range c.parts[i].lst {
			size += c.parts[i].lst[j].Size
		}
	}
	return n, size
}

func (c *collector) empty() bool {
	if c == nil {
		return true
	}
	for i := range c.parts {
		if len(c.parts[i].lst) > 0 {
			return false
		}
	}
	return true
}

// partition configures the collector to split
// inputs into partitions.
func (c *collector) init(parts []Partition) error {
	for i := range parts {
		field := parts[i].Field
		if field == "" {
			return fmt.Errorf("empty partition name")
		}
		// ensure there are no duplicates
		for j := i + 1; j < len(parts); j++ {
			if field == parts[j].Field {
				return fmt.Errorf("duplicate partition name %q", field)
			}
		}
		// ensure the field name can be used to
		// reference a capture group if a template
		// was not provided
		if parts[i].Value == "" {
			if !fsutil.ValidCaptureName(field) {
				return fmt.Errorf("cannot use field name %q as value template", field)
			}
		}
	}
	c.def = parts
	c.parts = c.parts[:0]
	maps.Clear(c.ind)
	return nil
}

func (c *collector) add(glob string, in blockfmt.Input) (*partition, error) {
	part, err := c.part(glob, in.Path)
	if err != nil {
		return nil, err
	}
	part.lst = append(part.lst, in)
	return part, nil
}

// match returns the name for the partition that
// an object with the given path would belong to
// when matched against glob.
//
// The returned slice is a shared buffer that
// will be overwritten by consecutive calls to
// this method. Callers should copy the contents
// as necessary.
func (c *collector) match(glob, path string) ([]byte, error) {
	found, err := c.mr.Match(glob, path)
	if err != nil {
		return nil, err
	} else if !found {
		// this shouldn't happen in practice because
		// the caller is expected to ensure the glob
		// pattern matches the input object path
		return nil, fmt.Errorf("path %q does not match pattern %q", path, glob)
	}
	// build the path prefix
	c.buf = c.buf[:0]
	for i := range c.def {
		if len(c.buf) > 0 {
			c.buf = append(c.buf, '/')
		}
		seg, err := c.expand(c.def[i].Field, c.def[i].Value)
		if err != nil {
			return nil, err
		}
		if !checkSegment(seg) {
			return nil, fmt.Errorf("bad path segment: %s", seg)
		}
		c.buf = append(c.buf, seg...)
	}
	return c.buf, nil
}

func (c *collector) part(glob, path string) (*partition, error) {
	name, err := c.match(glob, path)
	if err != nil {
		return nil, err
	}
	if i, ok := c.ind[string(name)]; ok {
		return &c.parts[i], nil
	}
	// we have to eval the constants for the
	// partition which involves expanding all the
	// templates again...
	var cons []ion.Field
	for i := range c.def {
		val, err := c.expand(c.def[i].Field, c.def[i].Value)
		if err != nil {
			// should already have been checked,
			// but just to be safe...
			return nil, err
		}
		d, err := evalconst(c.def[i].Type, val)
		if err != nil {
			return nil, err
		}
		cons = append(cons, ion.Field{
			Label: c.def[i].Field,
			Datum: d,
		})
	}
	str := string(c.buf)
	if c.ind == nil {
		c.ind = make(map[string]int)
	}
	c.ind[str] = len(c.parts)
	c.parts = append(c.parts, partition{
		name:    str,
		prepend: -1,
		cons:    cons,
	})
	return &c.parts[len(c.parts)-1], nil
}

// if a template is provided, expand calls
// p.mr.expand(template); otherwise expand
// calls p.mr.get(name)
func (c *collector) expand(name, template string) ([]byte, error) {
	if template != "" {
		return c.mr.Expand(template)
	}
	if name == "" {
		// should not be possible; we already
		// validated this at init time
		return nil, fmt.Errorf("neither field name nor template specified")
	}
	s := c.mr.Get(name)
	if s == "" {
		return nil, fmt.Errorf("no capture group %q in pattern", name)
	}
	// reuse the result buffer to avoid alloc...
	return append(c.mr.Result[:0], s...), nil
}

func evalconst(typ string, val []byte) (ion.Datum, error) {
	switch typ {
	case "string", "":
		return ion.String(string(val)), nil
	case "int":
		i, err := strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			return ion.Empty, err
		}
		return ion.Int(i), nil
	case "date":
		t, ok := date.Parse(append(val, "T00:00:00Z"...))
		if !ok {
			return ion.Empty, fmt.Errorf("invalid date %q", val)
		}
		return ion.Timestamp(t), nil
	case "datetime", "timestamp":
		t, ok := date.Parse(val)
		if !ok {
			return ion.Empty, fmt.Errorf("invalid datetime %q", val)
		}
		return ion.Timestamp(t), nil
	default:
		return ion.Empty, fmt.Errorf("invalid type %q", typ)
	}
}

// checkSegment is used to validate whether seg
// can be used as partition path segment.
func checkSegment(seg []byte) bool {
	for {
		s := seg
		i := bytes.IndexRune(s, '/')
		if i == utf8.RuneError {
			return false
		}
		if i >= 0 {
			s, seg = s[:i], s[i+1:]
		}
		if string(s) == "" || string(s) == "." || string(s) == ".." {
			return false
		}
		if i < 0 {
			return true
		}
	}
}
