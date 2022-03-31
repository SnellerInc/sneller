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
