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
	"errors"
	"io"
	"net"
)

// Attach takes a fresh connection to a remote
// tenant proxy and asks the remote proxy to
// attach this connection to the tenant
// given by id.
func Attach(dst net.Conn, id ID, key Key) error {
	var hdr header
	hdr.populate(id, key)
	_, err := dst.Write(hdr.body[:])
	return err
}

// Ping sends an Attach message with a zero
// tenant ID and waits for the remote end to
// close the connection.
func Ping(dst net.Conn) error {
	var zeroid ID
	err := Attach(dst, zeroid, Key{})
	if err != nil {
		return err
	}
	_, err = dst.Read(zeroid[:])
	if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}
