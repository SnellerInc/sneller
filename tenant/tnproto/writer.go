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
