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

//go:build !amd64
// +build !amd64

package atomicext

// Pause improves the performance of spin-wait loops. Not much can be done in the generic case to
// cancel the speculative memory accesses already in flight and to prevent the processor from restarting
// the contending code too soon. The noinline is used to ensure the processor executes at least the call.

//go:noinline
func Pause() {}
