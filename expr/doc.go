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
