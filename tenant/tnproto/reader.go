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
	HeaderSize = 96
	IDSize     = 24
	KeySize    = 32

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
	keyOffset   = idOffset + IDSize
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

// Key is the opaque tenant key.
// Key is used to authorize tenant requests.
type Key [KeySize]byte

func (k Key) String() string {
	return base64.URLEncoding.EncodeToString(k[:])
}

func (k Key) IsZero() bool { return k == Key{} }

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

func (h *header) Key() (key Key) {
	copy(key[:], h.body[keyOffset:])
	return
}

func (h *header) populate(id ID, key Key) {
	binary.LittleEndian.PutUint64(h.body[magicOffset:], headerMagic)
	copy(h.body[idOffset:], id[:])
	copy(h.body[keyOffset:], key[:])
}

// ReadHeader reads an Attach message from the
// provided connection and returns the requested ID,
// or an error if the message could not be read.
//
// See also: Attach
func ReadHeader(src net.Conn) (ID, Key, error) {
	var hdr header
	_, err := io.ReadFull(src, hdr.body[:])
	if err != nil {
		return ID{}, Key{}, err
	}
	if hdr.Key().IsZero() && !hdr.ID().IsZero() {
		return ID{}, Key{}, fmt.Errorf("zero key")
	}
	return hdr.ID(), hdr.Key(), hdr.validate()
}
