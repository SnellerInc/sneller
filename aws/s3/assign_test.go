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

package s3_test

import (
	"github.com/SnellerInc/sneller/aws/s3"
	"github.com/SnellerInc/sneller/fsutil"
)

// we can't do this inside the s3 package
// due to circular imports, but we can do it here:
var _ fsutil.WalkGlobFS = &s3.BucketFS{}
var _ fsutil.NamedFile = &s3.File{}
