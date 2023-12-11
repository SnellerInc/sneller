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
	"context"
	"fmt"
	"io/fs"
	"time"

	"github.com/SnellerInc/sneller/aws/s3"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

type OutputFS interface {
	blockfmt.UploadFS
}

type InputFS interface {
	blockfmt.InputFS
}

// ContextFS can be implemented by a file system
// which allows the file system to be configured
// with a context which will be applied to all
// file system operations.
type ContextFS interface {
	fs.FS
	// WithContext returns a copy of the file
	// system configured with the given context.
	WithContext(ctx context.Context) fs.FS
}

type aborter interface {
	Abort() error
}

// we expect to be able to abort s3 uploads
var _ aborter = &s3.Uploader{}

func abort(up blockfmt.Uploader) error {
	if a, ok := up.(aborter); ok {
		return a.Abort()
	}
	return nil
}

type etagger interface {
	ETag() string
}

var _ etagger = &s3.Uploader{}

// get the ETag and LastModified time of an output object
func getInfo(dst OutputFS, fp string, out blockfmt.Uploader) (string, time.Time, error) {
	// we can avoid racing against a concurrent write
	// if we grab the ETag directly from the uploader
	info, err := fs.Stat(dst, fp)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("getting ETag: %w", err)
	}
	lm := info.ModTime()
	etag, err := dst.ETag(fp, info)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("getting ETag: %w", err)
	}
	if e, ok := out.(etagger); ok && e.ETag() != etag {
		return "", time.Time{}, fmt.Errorf("etag %s from Stat disagrees with etag %s", etag, e.ETag())
	}
	return etag, lm, nil
}
