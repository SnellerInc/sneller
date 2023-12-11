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

//go:build !amd64
// +build !amd64

package atomicext

// Pause improves the performance of spin-wait loops. Not much can be done in the generic case to
// cancel the speculative memory accesses already in flight and to prevent the processor from restarting
// the contending code too soon. The noinline is used to ensure the processor executes at least the call.

//go:noinline
func Pause() {}
