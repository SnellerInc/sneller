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

package vm

import (
	"fmt"

	"github.com/SnellerInc/sneller/ion"
)

// Encodes a symbol ID, which is stored in bytecode.
func encodeSymbolID(id ion.Symbol) uint32 {
	if id >= (1<<28)-1 {
		panic(fmt.Sprintf("symbol id too large: %d", id))
	}

	encoded := uint32((id & 0x7F) | 0x80)
	id >>= 7
	for id != 0 {
		encoded = (encoded << 8) | (uint32(id) & 0x7F)
		id >>= 7
	}
	return encoded
}

func decodeSymbolID(value uint32) ion.Symbol {
	if (value & 0x80808080) == 0 {
		panic(fmt.Sprintf("the provided argument is not an encoded symbol ID: %d", value))
	}

	decoded := value & 0x7F
	value >>= 8

	for value != 0 {
		decoded = (decoded << 7) | (value & 0x7F)
		value >>= 8
	}

	return ion.Symbol(decoded)
}
