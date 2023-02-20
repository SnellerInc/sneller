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

package elastic_proxy

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

type JSONLiteral struct {
	// string, bool or float64
	Value any
}

var (
	ErrUnsupportedLiteralType = errors.New("unsupported literal type")
)

func NewJSONLiteral(v any) (JSONLiteral, error) {
	jl := JSONLiteral{}
	if err := jl.set(v); err != nil {
		return jl, err
	}
	return jl, nil
}

func (jl *JSONLiteral) String() string {
	if jl.Value == nil {
		return "NULL"
	}

	switch v := jl.Value.(type) {
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case time.Time:
		return "`" + v.Format(time.RFC3339Nano) + "`"
	case string:
		if strings.HasPrefix(v, "/") && strings.HasSuffix(v, "/") {
			return v
		}
		return `'` + strings.ReplaceAll(v, `'`, `\'`) + `'`
	default:
		panic("unsupported type in JSON literal")
	}
}

func (jl *JSONLiteral) UnmarshalJSON(data []byte) error {
	var v any

	// we prefer int64 over float64, so we'll explicitly
	// unmarshal as an int64 before doing the generic
	// unmarshal that prefers float64 over int64.
	var vi int64
	if err := json.Unmarshal(data, &vi); err != nil {
		if err = json.Unmarshal(data, &v); err != nil {
			return err
		}
	} else {
		v = vi
	}
	return jl.set(v)
}

func (jl *JSONLiteral) set(v any) error {
	switch v := v.(type) {
	case bool, string, float64, time.Time, int64:
		jl.Value = v
	case int:
		jl.Value = int64(v)
	default:
		return ErrUnsupportedLiteralType
	}
	return nil
}
