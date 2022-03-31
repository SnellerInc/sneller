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

package main

import (
	"context"
	"errors"
	"io"
)

var (
	ErrNotSupported = errors.New("not supported")
)

type MetaDataStore interface {
	GetCatalogs(ctx context.Context, prefix string) ([]string, error)
	GetDatabases(ctx context.Context, catalogName, prefix string) ([]string, error)
	GetTables(ctx context.Context, catalogName, databaseName, prefix string) ([]string, error)
	GetChunks(ctx context.Context, catalogName, databaseName, tableName string) ([]IonChunk, error)
}

type DataSource interface {
	ResolveDataSource(ctx context.Context, name string) (DataSource, error)
	GetChildren(ctx context.Context, prefix string) ([]DataSource, error)
}

type IonDataSource interface {
	GetChunks(ctx context.Context, prefix string) ([]IonChunk, error)
}

type IonChunk interface {
	GetId() string
	GetSize() uint64
	GetRange(ctx context.Context, offset, len uint64) (io.Reader, error)
}
