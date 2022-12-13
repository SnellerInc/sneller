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

package db

import (
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// Tenant is the set of information necessary
// to perform queries on behalf of a tenant.
type Tenant interface {
	// ID should return the unique ID of the tenant.
	ID() string

	// Key should return the key used
	// for verifying the integrity of database objects.
	Key() *blockfmt.Key

	// Root should return the root of the
	// storage for database objects.
	// The returned FS should implement
	// UploadFS if it supports writing.
	Root() (InputFS, error)

	// Split should trim the prefix off of pattern
	// that specifies the source filesystem and return
	// the result as an InputFS and the trailing glob
	// pattern that can be applied to the input to yield
	// the results.
	Split(pattern string) (InputFS, string, error)
}

// TenantConfig holds configuration for each
// tenant.
type TenantConfig struct {
	// MaxScanBytes is the maximum number of bytes
	// allowed to be scanned for each query. If
	// this is 0, there is no limit.
	MaxScanBytes uint64
}

// TenantConfigurable is a tenant that may provide
// preferred configuration.
type TenantConfigurable interface {
	Tenant

	// Config returns the configuration options
	// for this tenant. This may return nil to
	// indicate all defaults should be used.
	Config() *TenantConfig
}
