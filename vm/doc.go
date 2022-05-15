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
