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
