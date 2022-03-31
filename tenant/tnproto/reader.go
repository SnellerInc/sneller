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

package tnproto

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

const (
	HeaderSize = 64
	IDSize     = 24

	// MaxPayloadSize is the maximum
	// size of a tenant protocol message payload
	// (either a serialized AST expression or
	// a serialized subplan)
	MaxPayloadSize = (1 << 24) - 1
)

const (
	// layout of the payload message;
	// note that we've left some slack here
	// for extensibility
	magicOffset = 0
	magicSize   = 8
	idOffset    = magicOffset + magicSize
)

// mostly random, but choosing 0xf0 as the first byte
// means this cannot be confused for ion data
const headerMagic uint64 = 0xf02edb72b983e448

// ID is the (opaque) tenant identifier.
// ID is used to isolate query execution environments.
type ID [IDSize]byte

func (id ID) String() string {
	return base64.URLEncoding.EncodeToString(id[:])
}

var zeroID ID

func (id ID) IsZero() bool {
	return bytes.Equal(id[:], zeroID[:])
}

type header struct {
	body [HeaderSize]byte
}

func (h *header) validate() error {
	magic := binary.LittleEndian.Uint64(h.body[magicOffset:])
	if magic != headerMagic {
		return fmt.Errorf("magic %x is not valid header magic", magic)
	}
	return nil
}

func (h *header) ID() (id ID) {
	copy(id[:], h.body[idOffset:])
	return
}

func (h *header) populate(id ID) {
	binary.LittleEndian.PutUint64(h.body[magicOffset:], headerMagic)
	copy(h.body[idOffset:], id[:])
}

// ReadID reads an Attach message from the
// provided connection and returns the requested ID,
// or an error if the message could not be read.
//
// See also: Attach
func ReadID(src net.Conn) (ID, error) {
	var hdr header
	_, err := io.ReadFull(src, hdr.body[:])
	if err != nil {
		return ID{}, err
	}
	return hdr.ID(), hdr.validate()
}
