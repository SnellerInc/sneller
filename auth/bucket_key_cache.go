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

package auth

import (
	"sync"

	"github.com/SnellerInc/sneller/aws"
	"github.com/SnellerInc/sneller/aws/s3"
)

type bucketKeyCache struct {
	cache map[string]string
	lock  sync.RWMutex
}

func (c *bucketKeyCache) BucketKey(bucket string, rootKey *aws.SigningKey) (*aws.SigningKey, error) {
	region, err := c.bucketRegion(bucket, rootKey)
	if err != nil {
		return nil, err
	}
	if region == rootKey.Region {
		return rootKey, nil
	}
	return rootKey.InRegion(region), nil
}

func (c *bucketKeyCache) bucketRegion(bucket string, rootKey *aws.SigningKey) (string, error) {
	if r := c.cachedRegion(bucket); r != "" {
		return r, nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.cache == nil {
		c.cache = make(map[string]string)
	} else if r := c.regionNoLock(bucket); r != "" {
		return r, nil
	}
	region, err := s3.BucketRegion(rootKey, bucket)
	if err != nil {
		return "", err
	}
	c.cache[bucket] = region
	return region, nil
}

func (c *bucketKeyCache) cachedRegion(bucket string) string {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.regionNoLock(bucket)
}

func (c *bucketKeyCache) regionNoLock(bucket string) string {
	if c.cache != nil {
		if k, ok := c.cache[bucket]; ok {
			return k
		}
	}
	return ""
}
