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

// Package vm implements the core
// query-processing "physical operators"
// that process streams of ion-encoded
// data.
//
// Each of the "physical operators"
// (such as Filter, FilterDistinct, Project, etc.)
// implements QuerySink, and generally operators
// also accept a QuerySink into which they will
// write their outputs. Typically, each of
// the operators will inspect the interfaces implemented
// by the output QuerySink and choose a method of
// passing rows to that QuerySink that is more
// efficient than passing serialized rows via an
// io.WriteCloser.
//
// Data is fed to a chain of vm.QuerySink
// operators via a call to vm.SplitInput or vm.Table.WriteChunks,
// and the final output of the query sink is directed to
// an io.Writer that is wrapped with vm.LockedSink.
package vm
