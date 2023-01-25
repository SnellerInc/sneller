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
