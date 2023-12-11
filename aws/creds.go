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

package aws

import (
	"bufio"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
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

// AmbientCreds tries to find the AWS credentials from:
//
//  1. AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
//     and AWS_REGION/AWS_DEFAULT_REGION environment
//     variables (AWS_REGION takes precedence over
//     AWS_DEFAULT_REGION).
//  2. The config files in $HOME/.aws/config and
//     $HOME/.aws/credentials.
//
// Additionally, AmbientKey respects the following
// environment variables:
//   - AWS_CONFIG_FILE for the config file path
//   - AWS_SHARED_CREDENTIALS_FILE for the credentials
//     file path
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
func AmbientCreds() (id, secret, region, token string, err error) {
	envdefault := func(base string, env ...string) string {
		for _, e := range env {
			if x := os.Getenv(e); x != "" {
				return x
			}
		}
		return base
	}

	id = envdefault("", "AWS_ACCESS_KEY_ID")
	secret = envdefault("", "AWS_SECRET_ACCESS_KEY")
	region = envdefault("", "AWS_REGION", "AWS_DEFAULT_REGION")
	token = envdefault("", "AWS_SESSION_TOKEN")

	home, err := os.UserHomeDir()
	if err != nil {
		return "", "", "", "", fmt.Errorf("trying to find $HOME: %w", err)
	}

	profile := envdefault("default", "AWS_PROFILE", "AWS_DEFAULT_PROFILE")

	// Locations of the config/credentials file is document in
	// https://docs.aws.amazon.com/sdkref/latest/guide/file-location.html
	configfile := envdefault(filepath.Join(home, ".aws", "config"), "AWS_CONFIG_FILE")
	credentialsfile := envdefault(filepath.Join(home, ".aws", "credentials"), "AWS_SHARED_CREDENTIALS_FILE")

	if region == "" {
		f, err := os.Open(configfile)
		if err != nil {
			return "", "", "", "", err
		}
		defer f.Close()

		var ssoStartURL string
		err = scan(f, fmt.Sprintf("profile %s", profile), []scanspec{
			{"region", &region},
			{"sso_start_url", &ssoStartURL},
		})
		if err != nil {
			return "", "", "", "", err
		}
		if ssoStartURL != "" {
			return "", "", "", "", errors.New("SSO profiles are not supported")
		}
	}

	if id == "" || secret == "" {

		f, err := os.Open(credentialsfile)
		if err != nil {
			return "", "", "", "", err
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil {
			return "", "", "", "", fmt.Errorf("examining credentials: %w", err)
		}
		err = check(info)
		if err != nil {
			return "", "", "", "", err
		}
		err = scan(f, profile, []scanspec{
			{"aws_access_key_id", &id},
			{"aws_secret_access_key", &secret},
		})
		if err != nil {
			return "", "", "", "", err
		}

		// credentials file never contain a session token,
		// so it should be reset
		token = ""
	}
	if id == "" || secret == "" {
		return "", "", "", "", fmt.Errorf("unable to determine id or secret")
	}
	if region == "" {
		return "", "", "", "", fmt.Errorf("unable to determine region")
	}
	return
}

// WebIdentityCreds tries to load the credentials
// from a web-identity. The web-identity token should
// be stored in a file whose path is exposed in the
// AWS_WEB_IDENTITY_TOKEN_FILE environment variable.
// It will assume the role as specified in the
// AWS_ROLE_ARN environment variable.
func WebIdentityCreds(client *http.Client) (id, secret, region, token string, expiration time.Time, err error) {
	region = os.Getenv("AWS_REGION")
	if region == "" {
		return "", "", "", "", time.Time{}, fmt.Errorf("AWS_REGION not set")
	}

	roleSessionName := os.Getenv("AWS_ROLE_SESSION_NAME")
	if roleSessionName == "" {
		roleSessionName = "default"
	}

	roleARN := os.Getenv("AWS_ROLE_ARN")
	if roleARN == "" {
		return "", "", "", "", time.Time{}, fmt.Errorf("AWS_ROLE_ARN not set")
	}

	webIdentityTokenFile := os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE")
	if webIdentityTokenFile == "" {
		return "", "", "", "", time.Time{}, fmt.Errorf("AWS_WEB_IDENTITY_TOKEN_FILE not set")
	}

	webIdentityToken, err := os.ReadFile(webIdentityTokenFile)
	if err != nil {
		return "", "", "", "", time.Time{}, fmt.Errorf("can't read web-identity token from %q: %w", webIdentityTokenFile, err)
	}

	if client == nil {
		client = http.DefaultClient
	}

	u, _ := url.Parse("https://sts.amazonaws.com/?Action=AssumeRoleWithWebIdentity&Version=2011-06-15")
	q := u.Query()
	q.Add("RoleSessionName", roleSessionName)
	q.Add("RoleArn", roleARN)
	q.Add("WebIdentityToken", strings.TrimSpace(string(webIdentityToken)))
	u.RawQuery = q.Encode()

	resp, err := client.Do(&http.Request{
		Method: http.MethodGet,
		URL:    u,
		Header: http.Header{
			"Accept": []string{"application/xml"},
		},
	})
	if err != nil {
		return "", "", "", "", time.Time{}, fmt.Errorf("can't exchange web-identity token for AWS credentials: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", "", "", "", time.Time{}, fmt.Errorf("AssumeRoleWithWebIdentity returned HTTP status %s", resp.Status)
	}

	var result struct {
		Result struct {
			Credentials struct {
				AccessKeyID     string    `xml:"AccessKeyId"`
				SecretAccessKey string    `xml:"SecretAccessKey"`
				SessionToken    string    `xml:"SessionToken"`
				Expiration      time.Time `xml:"Expiration"`
			} `xml:"Credentials"`
		} `xml:"AssumeRoleWithWebIdentityResult"`
	}

	if err = xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", "", "", "", time.Time{}, err
	}

	creds := result.Result.Credentials

	id = creds.AccessKeyID
	secret = creds.SecretAccessKey
	token = creds.SessionToken
	expiration = creds.Expiration
	return
}

// AmbientKey tries to produce a signing key
// from the ambient filesystem, environment, etc.
// The key is derived using derive, unless it is nil,
// in which case DefaultDerive is used instead.
func AmbientKey(service string, derive DeriveFn) (*SigningKey, error) {
	if derive == nil {
		derive = DefaultDerive
	}

	id, secret, region, token, _, err := WebIdentityCreds(nil)
	if err != nil {
		id, secret, region, token, err = AmbientCreds()
		if err != nil {
			return nil, err
		}
	}

	baseURI := ""
	switch service {
	case "s3":
		baseURI = S3EndPoint(region)
	default:
		return nil, fmt.Errorf("unknown service %s", service)
	}

	return derive(baseURI, id, secret, token, region, service)
}

// S3EndPoint returns the endpoint of the object
// storage service.
func S3EndPoint(region string) string {
	endPoint := os.Getenv("S3_ENDPOINT")
	if endPoint == "" {
		endPoint = fmt.Sprintf("https://s3.%s.amazonaws.com", region)
	}
	endPoint = strings.TrimSuffix(endPoint, "/")
	return endPoint
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
		line := strings.TrimSpace(s.Text())
		matched = isSection(line, section, matched)
		if !matched {
			continue
		}
		// we are trying to match
		//   prefix (space*) '=' (space*) suffix
		for i := 0; i < len(into); i++ {
			before, after, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}

			before = strings.TrimSpace(before)
			if before == into[i].prefix {
				*into[i].dst = strings.TrimSpace(after)
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
