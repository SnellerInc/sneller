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

// Package expr implements the
// AST representation of query expressions.
//
// Each of the AST node types satisfies
// the Node interface.
//
// The critical entry points for this
// package are Walk, Check, and Simplify.
// Those routines allow a caller to examine
// the AST and collect output diagnostics
// or perform rewriting.
package expr
