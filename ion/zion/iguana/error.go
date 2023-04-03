// Copyright (C) 2023 Sneller, Inc.
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

package iguana

import (
	"errors"
)

type errorCode uint32

const (
	ecOK errorCode = iota
	ecCorruptedBitStream
	ecWrongSourceSize
	ecOutOfInputData
	ecInsufficientTargetCapacity
	ecUnrecognizedCommand
	ecLastCode
)

var errs = [ecLastCode]error{
	ecOK:                         nil,
	ecCorruptedBitStream:         errors.New("bitstream corruption detected"),
	ecWrongSourceSize:            errors.New("wrong source size"),
	ecOutOfInputData:             errors.New("out of input bytes"),
	ecUnrecognizedCommand:        errors.New("unrecognized command"),
	ecInsufficientTargetCapacity: errors.New("insufficient target capacity"),
}
