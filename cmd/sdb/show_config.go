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

package main

import (
	"encoding/base64"
	"fmt"
	"os"
	"strings"

	"github.com/SnellerInc/sneller/aws"
)

func notSet(v string) string {
	if v == "" {
		return "<not set>"
	}
	return v
}

func setVar(format, name, value string) {
	switch format {
	case "sh":
		fmt.Printf("export %s='%s'\n", name, value)
	case "pwsh":
		fmt.Printf("$env:%s='%s'\n", name, value)
	case "cmd":
		fmt.Printf("set %s=%s\n", name, value)
	default:
		panic(fmt.Sprintf("invalid format %q", format))
	}
}

// entry point for 'sdb show-config'
func showConfig(format string) {
	awsAccessKeyID, awsSecretAccessKey, awsRegion, token, err := aws.AmbientCreds()
	if err != nil {
		exitf("Unable to determine AWS credentials: %s", err)
		return
	}
	c := creds()
	rootFS, err := c.Root()
	if err != nil {
		exitf("Unable to determine storage root: %s", err)
		return
	}

	encodedKey := ""
	if c.Key() != nil {
		encodedKey = base64.StdEncoding.EncodeToString((*c.Key())[:])
	}

	switch format {
	case "text":
		fmt.Printf("           AWS region: %s\n", notSet(awsRegion))
		fmt.Printf("    AWS access key ID: %s\n", notSet(awsAccessKeyID))
		fmt.Printf("AWS secret access key: %s\n", notSet(awsSecretAccessKey))
		fmt.Printf("    AWS session token: %s\n", notSet(token))
		fmt.Printf("          S3 endpoint: %s\n", notSet(aws.S3EndPoint(awsRegion)))
		fmt.Printf("       Storage prefix: %s\n", notSet(rootFS.Prefix()))
		fmt.Printf("            Index key: %s\n", notSet(encodedKey))

	case "sh", "pwsh", "cmd":
		setVar(format, "AWS_REGION", awsRegion)
		setVar(format, "AWS_DEFAULT_REGION", awsRegion)
		setVar(format, "AWS_ACCESS_KEY_ID", awsAccessKeyID)
		setVar(format, "AWS_SECRET_ACCESS_KEY", awsSecretAccessKey)
		setVar(format, "AWS_SESSION_TOKEN", token)
		setVar(format, "AWS_SECURITY_TOKEN", "")
		setVar(format, "S3_ENDPOINT", aws.S3EndPoint(awsRegion))
		setVar(format, "SNELLER_BUCKET", strings.TrimSuffix(rootFS.Prefix(), "/"))
		setVar(format, "SNELLER_INDEX_KEY", encodedKey)

	default:
		fmt.Fprintf(os.Stderr, "Invalid format %q", format)
	}
}

func init() {
	addApplet(applet{
		name: "show-config",
		help: "[text|pwsh|sh|cmd]",
		desc: `show the AWS settings that are used by SDB`,
		run: func(args []string) bool {
			if len(args) < 1 || len(args) > 2 {
				return false
			}
			format := "text"
			if len(args) == 2 {
				format = args[1]
			}
			showConfig(format)
			return true
		},
	})
}
