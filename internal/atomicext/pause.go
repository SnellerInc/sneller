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

package atomicext

// Pause improves the performance of spin-wait loops. When executing a "spin-wait loop," processors will suffer a severe
// performance penalty when exiting the loop because it detects a possible memory order violation. The Pause
// function provides a hint to the processor that the code sequence is a spin-wait loop. The processor uses this hint
// to avoid the memory order violation in most situations, which greatly improves processor performance. For this
// reason, it is recommended that a Pause instruction be placed in all spin-wait loops. [paraphrasing the Intel SDM, vol. 2B 4-235]
//
//go:noescape
//go:nosplit
func Pause()
