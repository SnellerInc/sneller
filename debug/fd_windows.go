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

//go:build !linux

// Package debug provides remote debugging tools
package debug

import (
	"log"
)

// Fd binds an http server to the
// provided file descriptor and starts
// it asynchronously. If the server ever
// stops running, the error returned
// from http.Serve is passed to errorln.
func Fd(fd int, lg *log.Logger) {
	panic("unimplemented")
}
