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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var testNow *time.Time

func formatIn(key string, value any, mapping map[string]TypeMapping) (any, error) {
	f, ok := format(key, mapping)
	if !ok {
		return value, nil
	}

	var err error
	switch f {
	case "datetime":
		switch v := value.(type) {
		case time.Time:
			return v.UTC(), nil
		case string:
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				t, err = time.Parse(time.RFC3339, v)
				if err != nil {
					now := time.Now()
					if testNow != nil {
						now = *testNow
					}
					t, err = ParseDateMath(v, now)
					if err != nil {
						return nil, err
					}
				}
			}
			return t.UTC(), nil
		}

	case "unix_seconds":
		var sec int64
		switch v := value.(type) {
		case time.Time:
			return v, nil
		case string:
			sec, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid unix_seconds %q", v)
			}
		case int:
			sec = int64(v)
		case int64:
			sec = v
		default:
			return nil, errors.New("invalid source-type for unix_seconds")
		}
		return time.Unix(sec, 0).UTC(), nil
	case "unix_milli_seconds":
		var msec int64
		switch v := value.(type) {
		case time.Time:
			return v, nil
		case string:
			msec, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid unix_milli_seconds %q", v)
			}
		case int:
			msec = int64(v)
		case int64:
			msec = v
		default:
			return nil, errors.New("invalid source-type for unix_milli_seconds")
		}
		return time.UnixMilli(msec).UTC(), nil
	case "unix_micro_seconds":
		var usec int64
		switch v := value.(type) {
		case time.Time:
			return v, nil
		case string:
			usec, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid unix_micro_seconds %q", v)
			}
		case int:
			usec = int64(v)
		case int64:
			usec = v
		default:
			return nil, errors.New("invalid source-type for unix_micro_seconds")
		}
		return time.UnixMicro(usec).UTC(), nil
	case "unix_nano_seconds":
		var nsec int64
		switch v := value.(type) {
		case time.Time:
			return v, nil
		case string:
			nsec, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid unix_nano_seconds %q", v)
			}
		case int:
			nsec = int64(v)
		case int64:
			nsec = v
		case float64:
			nsec = int64(v)
		default:
			return nil, errors.New("invalid source-type for unix_nano_seconds")
		}
		// round to the nearest microsecond to make
		// sure it matches with the internal Sneller precision
		nsec = (nsec / 1000) * 1000
		return time.Unix(0, nsec).UTC(), nil
	}

	return nil, fmt.Errorf("field %q has invalid type-format %q", key, f)
}

func formatOut(key string, value any, mapping map[string]TypeMapping) (any, error) {
	f, ok := format(key, mapping)
	if !ok {
		if _, ok := value.(time.Time); ok {
			return formatOutRaw(value, "datetime")
		}
		return value, nil
	}

	return formatOutRaw(value, f)
}

func mapType(key string, mapping map[string]TypeMapping) (*TypeMapping, bool) {
	if m, ok := mapping[key]; ok {
		return &m, true
	}

	keyLen := 0
	var tm TypeMapping
	for k, m := range mapping {
		if matchWildcard(key, k) {
			if len(k) > keyLen {
				tm = m
				keyLen = len(k)
			}
		}
	}
	if tm.Type != "" {
		return &tm, true
	}

	return nil, false
}

func format(key string, mapping map[string]TypeMapping) (string, bool) {
	m, ok := mapType(key, mapping)
	if ok {
		return m.Type, true
	}
	return "", false
}

func formatOutRaw(value any, f string) (any, error) {
	if f == "" {
		return value, nil
	}

	switch vv := value.(type) {
	case []any:
		l := make([]any, 0, len(vv))
		for _, v := range vv {
			vf, err := formatOutRaw(v, f)
			if err != nil {
				return nil, err
			}
			l = append(l, vf)
		}
		return l, nil

	case string, *string:
		return value, nil

	case time.Time:
		switch f {
		case "basic_date":
			return vv.Format("20060102"), nil
		case "basic_date_time":
			return vv.Format("20060102T15:04:05.999Z"), nil
		case "basic_date_time_no_millis":
			return vv.Format("20060102T15:04:05Z"), nil
		case "basic_time":
			return vv.Format("15:04:05.999Z"), nil
		case "basic_time_no_millis":
			return vv.Format("15:04:05Z"), nil
		case "basic_t_time":
			return vv.Format("T15:04:05.999Z"), nil
		case "basic_t_time_no_millis":
			return vv.Format("T15:04:05Z"), nil
		case "date", "strict_date":
			return vv.Format("2006-01-02"), nil
		case "date_hour", "strict_date_hour":
			return vv.Format("2006-01-02T15"), nil
		case "date_hour_minute", "strict_date_hour_minute":
			return vv.Format("2006-01-02T15:04"), nil
		case "date_hour_minute_second_fraction ", "strict_date_hour_minute_second":
			return vv.Format("2006-01-02T15:04:05"), nil
		case "date_hour_minute_second", "strict_date_hour_minute_second_fraction", "date_hour_minute_second_millis ", "strict_date_hour_minute_second_millis":
			return vv.Format("2006-01-02T15:04:05.999"), nil
		case "date_time", "strict_date_time":
			return vv.Format("2006-01-02T15:04:05.999Z"), nil
		case "date_time_no_millis", "strict_date_time_no_millis":
			return vv.Format("2006-01-02T15:04:05"), nil
		case "datetime":
			t := vv.Format("2006-01-02T15:04:05.999999999Z")
			if !strings.ContainsRune(t, '.') {
				t = t[:len(t)-1] + ".000Z"
			}
			return t, nil
		case "epoch_second", "unix_seconds":
			return vv.Unix(), nil
		case "epoch_millis", "unix_milli_seconds":
			return vv.UnixMilli(), nil
		case "unix_micro_seconds":
			return vv.UnixMicro(), nil
		case "unix_nano_seconds":
			return vv.UnixNano(), nil
		default:
			return vv.Format("2006-01-02T15:04:05.999999999Z"), nil
		}
	}

	switch vv := value.(type) {
	case int64:
		// attempt formatting as date/time
		// TODO: How do we know that we should use epoch-ms???
		v, err := formatOutRaw(time.UnixMilli(vv).UTC(), f)
		if err == nil {
			return v, nil
		}
	}

	return nil, fmt.Errorf("type-format %q is unknown", f)
}
