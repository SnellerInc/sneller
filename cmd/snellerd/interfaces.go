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
