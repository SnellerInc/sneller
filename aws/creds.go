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

package aws

import (
	"bufio"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// DeriveFn is a function that can
// be used to derive a signing key
// from an endpoint, key ID, secret,
// region, and service.
//
// The simplest implementation of DeriveFn
// is just a call to DeriveKey, but more complex
// DeriveFn implementations can tweak the scope
// (region and service).
//
// See, for example, s3.DeriveForBucket.
type DeriveFn func(baseURI, id, secret, token, region, service string) (*SigningKey, error)

// DefaultDerive is the DeriveFn that
// simply calls DeriveKey and populates
// the session token if it is present.
func DefaultDerive(baseURI, id, secret, token, region, service string) (*SigningKey, error) {
	k := DeriveKey(baseURI, id, secret, region, service)
	k.Token = token
	return k, nil
}

// AmbientKey tries to produce a signing key
// from the ambient filesystem, environment, etc.
// The key is derived using derive, unless it is nil,
// in which case DefaultDerive is used instead.
//
// Keys are searched for in the following order:
//
//  1. AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
//     and AWS_DEFAULT_REGION environment variables
//  2. The config files in $HOME/.aws/config and
//     $HOME/.aws/credentials, with the credentials
//     file taking precedence over the config file.
//     (The path to the config file can be overridden
//     with the AWS_CONFIG_FILE environment variable.)
//
// Additionally, AmbientKey respects the following
// environment variables:
//   - AWS_CONFIG_FILE for the config file path
//   - AWS_PROFILE for the name of the profile
//     to search for in config files (otherwise "default")
//
// NOTE: in general, it is a bad idea to use
// "Do-What-I-Mean" functionality to load security
// credentials, because it's easy to accidentally
// load the wrong thing. Consider whether there
// may be safer alternatives. In general this method
// is safer than the aws SDK's "NewSession" function
// but less safe than explicitly picking up secrets
// from where you expect to find them. Caveat emptor.
func AmbientKey(service string, derive DeriveFn) (*SigningKey, error) {
	var id, secret, region, token string
	if derive == nil {
		derive = DefaultDerive
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("trying to find $HOME: %w", err)
	}
	envdefault := func(base, env string) string {
		if x := os.Getenv(env); x != "" {
			return env
		}
		return base
	}
	profile := envdefault("default", "AWS_PROFILE")
	configfile := envdefault(filepath.Join(home, ".aws", "credentials"), "AWS_CONFIG_FILE")

	if x := os.Getenv("AWS_ACCESS_KEY_ID"); x != "" {
		id = x
	}
	if x := os.Getenv("AWS_SECRET_ACCESS_KEY"); x != "" {
		secret = x
	}
	if x := os.Getenv("AWS_DEFAULT_REGION"); x != "" {
		region = x
	}
	if x := os.Getenv("AWS_SESSION_TOKEN"); x != "" {
		token = x
	}

	if id != "" && secret != "" && region != "" {
		s3baseURI := os.Getenv("S3_ENDPOINT")
		return derive(s3baseURI, id, secret, token, region, service)
	}

	info, err := os.Stat(filepath.Dir(configfile))
	if err != nil {
		return nil, fmt.Errorf("examining %s: %w", filepath.Dir(configfile), err)
	}
	err = check(info)
	if err != nil {
		return nil, err
	}

	if id == "" || secret == "" {
		f, err := os.Open(configfile)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("examinig credentials: %w", err)
		}
		err = check(info)
		if err != nil {
			return nil, err
		}
		err = scan(f, profile, []scanspec{
			{"aws_access_key_id", &id},
			{"aws_secret_access_key", &secret},
			{"region", &region},
			{"aws_session_token", &token},
		})
		if err != nil {
			return nil, err
		}
	}
	if region == "" {
		f, err := os.Open(filepath.Join(home, ".aws", "config"))
		if err != nil {
			return nil, err
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("examining config: %w", err)
		}
		err = check(info)
		if err != nil {
			return nil, err
		}
		err = scan(f, profile, []scanspec{
			{"region", &region},
		})
		if err != nil {
			return nil, err
		}
	}
	if id == "" || secret == "" {
		return nil, fmt.Errorf("unable to determine id or secret")
	}
	if region == "" {
		return nil, fmt.Errorf("unable to determine region")
	}
	return derive("", id, secret, token, region, service)
}

type scanspec struct {
	prefix string
	dst    *string
}

func isSection(line, section string, matched bool) bool {
	line = strings.TrimSpace(line)
	if len(line) < 2 || line[0] != '[' || line[len(line)-1] != ']' {
		return matched
	}
	return section == strings.TrimSpace(line[1:len(line)-1])
}

func scan(in io.Reader, section string, into []scanspec) error {
	s := bufio.NewScanner(in)
	matched := false
	for s.Scan() && len(into) > 0 {
		line := s.Text()
		line = strings.TrimSpace(line)
		matched = isSection(line, section, matched)
		if !matched {
			continue
		}
		// we are trying to match
		//   prefix (space*) '=' (space*) suffix
		for i := 0; i < len(into); i++ {
			if strings.HasPrefix(line, into[i].prefix) {
				// chomp prefix, chomp space*
				rest := strings.TrimSpace(strings.TrimPrefix(line, into[i].prefix))
				if len(rest) == 0 || rest[0] != '=' {
					continue
				}
				// chomp '='
				rest = rest[1:]
				// chomp space*
				rest = strings.TrimSpace(rest)
				*into[i].dst = rest
				into[i], into = into[len(into)-1], into[:len(into)-1]
			}
		}
	}
	if len(into) > 0 {
		return s.Err()
	}
	return nil
}

// we don't allow credentials to be loaded
// from world-writeable locations
func check(info fs.FileInfo) error {
	mode := info.Mode()
	if mode&2 != 0 {
		return fmt.Errorf("%s is world-writeable %o", info.Name(), mode)
	}
	if kind := mode & fs.ModeType; kind != fs.ModeDir && kind != 0 {
		return fmt.Errorf("%s is a special file", info.Name())
	}
	return nil
}

// EC2Role derives a signing key
// from the name of a role that
// is available through EC2 instance metadata.
//
// 'Role' should be the full path to
// the EC2 metadata, so it will typically
// begin with "iam/security-credentials/"
// followed by the name of the role.
func EC2Role(role, service string, derive DeriveFn) (*SigningKey, error) {
	if derive == nil {
		derive = DefaultDerive
	}
	k := struct {
		Code            string    `json:"Code"`
		LastUpdated     time.Time `json:"LastUpdated"`
		Type            string    `json:"Type"`
		AccessKeyID     string    `json:"AccessKeyId"`
		SecretAccessKey string    `json:"SecretAccessKey"`
		Token           string    `json:"Token"`
		Expiration      time.Time `json:"Expiration"`
	}{}
	err := MetadataJSON(role, &k)
	if err != nil {
		return nil, err
	}
	region, err := ec2Region()
	if err != nil {
		return nil, err
	}
	sk, err := derive("", k.AccessKeyID, k.SecretAccessKey, k.Token, region, service)
	if err != nil {
		return nil, err
	}
	sk.Token = k.Token
	return sk, nil
}
