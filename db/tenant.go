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
