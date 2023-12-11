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

package proxy_http

import (
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"testing"
	"time"

	elastic_proxy "github.com/SnellerInc/sneller/elasticproxy/elastic-proxy"

	"github.com/bradfitz/gomemcache/memcache"
)

func TestMemcacheMappingCache(t *testing.T) {
	client := memcached(t)
	cache := NewMemcacheMappingCache(client, "Test", t.Name(), 0)
	mapping := &elastic_proxy.ElasticMapping{
		Properties: elastic_proxy.Properties{
			"task": {
				Type: "string",
			},
			"duration": {
				Type: "float",
			},
		},
	}

	t.Run("get nonexisting", func(t *testing.T) {
		m, err := cache.Fetch("foobar")
		if err != nil {
			t.Fatal(err)
		}

		if m != nil {
			t.Errorf("no value should be returned")
		}
	})

	t.Run("store and load", func(t *testing.T) {
		err := cache.Store("foobar", mapping)
		if err != nil {
			t.Fatal(err)
		}

		got, err := cache.Fetch("foobar")
		if err != nil {
			t.Fatal(err)
		}

		if got == nil {
			t.Errorf("value should be returned")
			return
		}

		if !reflect.DeepEqual(got, mapping) {
			t.Logf("got : %v", got)
			t.Logf("want: %v", mapping)
			t.Errorf("fetched mapping is different than the one previously stored")
		}
	})
}

func memcached(t *testing.T) *memcache.Client {
	bin, err := exec.LookPath("memcached")
	if err != nil {
		t.Skip("cannot find memcached:", err)
	}

	port := 12345
	cmd := exec.Command(bin, "-X", "-W", "-l", "127.0.0.1", "-p", strconv.Itoa(port))
	err = cmd.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Signal(os.Kill)
		cmd.Wait()
	})

	client := memcache.New(fmt.Sprintf("127.0.0.1:%d", port))
	for { // wait for start
		err := client.Ping()
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	return client
}
