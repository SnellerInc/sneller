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
	"testing"

	"github.com/SnellerInc/sneller/aws"
)

func TestSplit(t *testing.T) {
	s := S3Resolver{
		DeriveKey: func(_ string) (*aws.SigningKey, error) {
			return nil, nil
		},
	}
	fs, rest, err := s.Split("s3://bucket.name/object/key")
	if err != nil {
		t.Fail()
	}
	s3fs, ok := fs.(*S3FS)
	if !ok || s3fs.Bucket != "bucket.name" || rest != "object/key" {
		t.Fail()
	}
}
