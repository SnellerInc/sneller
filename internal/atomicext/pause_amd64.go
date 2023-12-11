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

//go:build amd64
// +build amd64

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
