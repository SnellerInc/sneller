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

// static bytecode register assignments:
//  AX = bytecode dispatch array
//  SI = buffer base
//  DI = &wherebc structure
//  R11 = mask save buffer
//  R12 = value save buffer
//  R9, R10 = callee-save (do not clobber)
//  'current row:'
//  Z0 = struct base
//  Z1 = struct len
//  'current scalar:'
//  Z2 + Z3 = current scalar
//  'current value'
//  Z30 = this field base (from findsym)
//  Z31 = this field len  (from findsym)
//  K1 = current mask bits
//  K7 = valid mask bits

#define VIRT_PCREG  AX  // points to "bytecode"
#define VIRT_BCPTR  DI
#define VIRT_BASE   SI
#define VIRT_VALUES R12 // points to a value stack

#define VREG_SIZE 128

// BCCLEARSCRATCH resets the output scratch buffer
#define BCCLEARSCRATCH(tmp) \
  MOVQ    bytecode_scratchreserve(VIRT_BCPTR), tmp   \
  MOVQ    tmp, bytecode_scratch+8(VIRT_BCPTR)

// BCCLEARERROR resets error code of bytecode program
#define BCCLEARERROR() \
  MOVL $0, bytecode_err(VIRT_BCPTR)

// VMINVOKE implements the lowest-level VM entry mechanism. All the required registers must be preset by the caller.
#define VMINVOKE()      \
  ADDQ $8, VIRT_PCREG   \
  CALL -8(VIRT_PCREG)

// VMENTER() sets up the VM control registers and jumps into the VM instructions
// (VIRT_BCPTR must be set to the *bytecode pointer)
//
//  VMENTER() also takes care to reset the output scratch buffer
#define VMENTER()                                 \
  KMOVW K1, K7                                    \
  BCCLEARSCRATCH(VIRT_PCREG)                      \
  MOVQ bytecode_compiled(VIRT_BCPTR), VIRT_PCREG  \
  MOVQ bytecode_vstack(VIRT_BCPTR), VIRT_VALUES   \
  VMINVOKE()

// VM_GET_SCRATCH_BASE_ZMM(dst, mask) sets dst.mask
// to the current scratch base (equal in all lanes);
// this address can be scattered to safely as long
// as the scratch capacity has been checked in advance
#define VM_GET_SCRATCH_BASE_ZMM(dst, mask) \
  VPBROADCASTD  bytecode_scratchoff(VIRT_BCPTR), mask, dst \
  VPADDD.BCST   bytecode_scratch+8(VIRT_BCPTR), dst, mask, dst

#define VM_GET_SCRATCH_BASE_GP(dst) \
  MOVLQSX bytecode_scratchoff(VIRT_BCPTR), dst \
  ADDQ bytecode_scratch+8(VIRT_BCPTR), dst

#define VM_CHECK_SCRATCH_CAPACITY(size, sizereg, abrt) \
  MOVQ bytecode_scratch+16(VIRT_BCPTR), sizereg \
  SUBQ bytecode_scratch+8(VIRT_BCPTR), sizereg \
  CMPQ sizereg, size \
  JLT  abrt
