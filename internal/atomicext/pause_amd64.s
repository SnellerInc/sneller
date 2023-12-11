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

#include "textflag.h"
#include "funcdata.h"
#include "go_asm.h"

// func Pause()
TEXT ·Pause(SB), NOSPLIT | NOFRAME, $0-0
    PAUSE // Per the ISA encoding, this is an alias to REP NOP. Does nothing except for the memory ordering side effects.
    RET
