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
