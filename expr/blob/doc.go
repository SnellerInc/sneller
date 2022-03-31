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

// Package blob implements an
// interface through which the
// query planner can assign "blobs"
// of data for the query execution
// engine to consume.
//
// Typically there isn't a strict 1:1
// relationship between the backing
// store for query data (S3 objects, for example)
// and blobs. This is by design: we can
// make this relationship many-to-one or
// one-to-many in order to keep the size
// of a "blob" mostly uniform, which makes
// them easier to cache.
package blob
