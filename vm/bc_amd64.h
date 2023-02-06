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
#define VIRT_AGG_BUFFER R10 // points to aggregate data buffer

#define BC_VSTACK_PTR(Slot, Offset) (Offset)(VIRT_VALUES)(Slot*1)

// NOTE: It's not enough to change the following macros to get 4 bytes per slot,
// there are some optimizations in BC_UNPACK_2xSLOT(), etc... that current take
// advantage of 2 bytes per slot, these would have to be changed to make slots
// 4 bytes long.

#define BC_SLOT_SIZE 2      // Size of a stack slot, in bytes
#define BC_DICT_SIZE 2      // Size of a dictionary reference

#define BC_MOV_SLOT MOVWLZX // Instruction to load a stack slot
#define BC_MOV_DICT MOVWLZX // Instruction to load a stack slot

#define VREG_SIZE const_vRegSize       // Size of a V register

// Generic Macros
// --------------

// Fills the destination register with ones
#define BC_FILL_ONES(Out) VPTERNLOGD $0xFF, Out, Out, Out

// BC Interpreter
// --------------

// BC_INVOKE implements the lowest-level VM entry mechanism. All the
// required registers must be preset by the caller.
#define BC_INVOKE()   \
  ADDQ $8, VIRT_PCREG \
  CALL -8(VIRT_PCREG)

// BC_ENTER() sets up the VM control registers and jumps into the VM
// instructions (VIRT_BCPTR must be set to the *bytecode pointer)
//
// BC_ENTER() also takes care to reset the output scratch buffer
#define BC_ENTER()                                             \
  KMOVW K1, K7                                                 \
  BC_CLEAR_SCRATCH(VIRT_PCREG)                                 \
  MOVQ bytecode_compiled(VIRT_BCPTR), VIRT_PCREG               \
  MOVQ bytecode_vstack(VIRT_BCPTR), VIRT_VALUES                \
  BC_INVOKE()

// BC Scratch Buffer
// -----------------

// BC_CLEAR_SCRATCH resets the output scratch buffer:
// len(bytecode.scratch) = len(bytecode.savedlit)
#define BC_CLEAR_SCRATCH(tmp)                                  \
  MOVQ bytecode_savedlit+8(VIRT_BCPTR), tmp                    \
  MOVQ tmp, bytecode_scratch+8(VIRT_BCPTR)

// BC_GET_SCRATCH_BASE_ZMM(dst, mask) sets dst.mask
// to the current scratch base (equal in all lanes);
// this address can be scattered to safely as long
// as the scratch capacity has been checked in advance
#define BC_GET_SCRATCH_BASE_GP(dst)                            \
  MOVLQSX bytecode_scratchoff(VIRT_BCPTR), dst                 \
  ADDQ bytecode_scratch+8(VIRT_BCPTR), dst

#define BC_GET_SCRATCH_BASE_ZMM(dst, mask)                     \
  VPBROADCASTD bytecode_scratchoff(VIRT_BCPTR), mask, dst      \
  VPADDD.BCST bytecode_scratch+8(VIRT_BCPTR), dst, mask, dst

#define BC_CHECK_SCRATCH_CAPACITY(size, sizereg, abrt)         \
  MOVQ bytecode_scratch+16(VIRT_BCPTR), sizereg                \
  SUBQ bytecode_scratch+8(VIRT_BCPTR), sizereg                 \
  CMPQ sizereg, size \
  JLT  abrt

// BC Error Handling
// -----------------

// BC_CLEAR_ERROR resets error code of bytecode program
#define BC_CLEAR_ERROR() MOVL $0, bytecode_err(VIRT_BCPTR)

#define _BC_ERROR_HANDLER_MORE_SCRATCH()                       \
error_handler_more_scratch:                                    \
  MOVL $const_bcerrMoreScratch, bytecode_err(VIRT_BCPTR)       \
  RET_ABORT()

#define _BC_ERROR_HANDLER_NULL_SYMTAB()                        \
error_null_symtab:                                             \
  MOVL $const_bcerrNullSymbolTable, bytecode_err(VIRT_BCPTR)   \
  RET_ABORT()

// BC Instruction Unpack Helpers
// -----------------------------

// UNPACKing refers to extracting stack slots and immediate values from the
// current PC register, which points to the currently processed BC instruction.
//
// Terminology:
//   - SLOT - A slot stored with BC instruction, references a stack location
//   - MASK - Mask immediate, loaded to K register (should be all 0 or all 1)
//   - DICT - Dictionary string reference, references a go string struct
//   - Rxxx - GP immediate value, load to either 32-bit or 64-bit GP register
//   - Zxxx - ZMM immediate value stored with BC instruction, broadcasted

#define BC_UNPACK_SLOT(Offset, DstR) BC_MOV_SLOT (Offset)(VIRT_PCREG), DstR
#define BC_UNPACK_MASK(Offset, DstK) KMOVW (Offset)(VIRT_PCREG), DstK
#define BC_UNPACK_ZI8(Offset, DstZ)  VPBROADCASTB (Offset)(VIRT_PCREG), DstZ
#define BC_UNPACK_ZI16(Offset, DstZ) VPBROADCASTW (Offset)(VIRT_PCREG), DstZ
#define BC_UNPACK_ZI32(Offset, DstZ) VPBROADCASTD (Offset)(VIRT_PCREG), DstZ
#define BC_UNPACK_ZI64(Offset, DstZ) VPBROADCASTQ (Offset)(VIRT_PCREG), DstZ
#define BC_UNPACK_ZF64(Offset, DstZ) VBROADCASTSD (Offset)(VIRT_PCREG), DstZ
#define BC_UNPACK_RU16(Offset, DstR) MOVWLZX (Offset)(VIRT_PCREG), DstR
#define BC_UNPACK_RU32(Offset, DstR) MOVL (Offset)(VIRT_PCREG), DstR
#define BC_UNPACK_RU64(Offset, DstR) MOVQ (Offset)(VIRT_PCREG), DstR

// DstR = imm * sizeof(string) + DictBase
#define BC_UNPACK_DICT(Offset, DstR)                                           \
  BC_MOV_DICT (Offset)(VIRT_PCREG), DstR                                       \
  SHLQ $4, DstR                                                                \
  ADDQ bytecode_dict(DI), DstR

#define BC_UNPACK_2xSLOT(Offset, DstR1, DstR2)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*0, DstR1)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*1, DstR2)

#define BC_UNPACK_3xSLOT(Offset, DstR1, DstR2, DstR3)                          \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*0, DstR1)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*1, DstR2)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*2, DstR3)

#define BC_UNPACK_4xSLOT(Offset, DstR1, DstR2, DstR3, DstR4)                   \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*0, DstR1)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*1, DstR2)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*2, DstR3)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*3, DstR4)

#define BC_UNPACK_5xSLOT(Offset, DstR1, DstR2, DstR3, DstR4, DstR5)            \
  BC_UNPACK_4xSLOT(Offset, DstR1, DstR2, DstR3, DstR4)                         \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*4, DstR5)

#define BC_UNPACK_SLOT_MASK_SLOT(Offset, DstR1, DstK2, DstR3)                  \
  BC_UNPACK_SLOT(Offset, DstR1)                                                \
  BC_UNPACK_MASK(Offset+BC_SLOT_SIZE*1, DstK2)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*1 + 2, DstR3)

#define BC_UNPACK_SLOT_RU16_SLOT(Offset, DstR1, DstR2, DstR3)                  \
  BC_UNPACK_SLOT(Offset, DstR1)                                                \
  BC_UNPACK_RU16(Offset+BC_SLOT_SIZE*1, DstR2)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*1 + 2, DstR3)

#define BC_UNPACK_SLOT_ZF64_SLOT(Offset, DstR1, DstZ2, DstR3)                  \
  BC_UNPACK_SLOT(Offset, DstR1)                                                \
  BC_UNPACK_ZF64(Offset+BC_SLOT_SIZE*1, DstZ2)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*1 + 8, DstR3)

#define BC_UNPACK_SLOT_ZI64_SLOT(Offset, DstR1, DstZ2, DstR3)                  \
  BC_UNPACK_SLOT(Offset, DstR1)                                                \
  BC_UNPACK_ZI64(Offset+BC_SLOT_SIZE*1, DstZ2)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*1 + 8, DstR3)

#define BC_UNPACK_SLOT_DICT_SLOT(Offset, DstR1, DstR2, DstR3)                  \
  BC_UNPACK_SLOT(Offset, DstR1)                                                \
  BC_UNPACK_DICT(Offset+BC_SLOT_SIZE*1, DstR2)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*1 + BC_DICT_SIZE, DstR3)

#define BC_UNPACK_2xSLOT_RU16_SLOT(Offset, DstR1, DstR2, DstR3, DstR4)         \
  BC_UNPACK_2xSLOT(Offset, DstR1, DstR2)                                       \
  BC_UNPACK_RU16(Offset+BC_SLOT_SIZE*2, DstR3)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*2 + 2, DstR4)

#define BC_UNPACK_2xSLOT_MASK_SLOT(Offset, DstR1, DstR2, DstK3, DstR4)         \
  BC_UNPACK_2xSLOT(Offset, DstR1, DstR2)                                       \
  BC_UNPACK_MASK(Offset+BC_SLOT_SIZE*2, DstK3)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*2 + 2, DstR4)

#define BC_UNPACK_2xSLOT_ZF64_SLOT(Offset, DstR1, DstR2, DstZ3, DstR4)         \
  BC_UNPACK_2xSLOT(Offset, DstR1, DstR2)                                       \
  BC_UNPACK_ZF64(Offset+BC_SLOT_SIZE*2, DstZ3)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*2 + 8, DstR4)

#define BC_UNPACK_2xSLOT_ZI64_SLOT(Offset, DstR1, DstR2, DstZ3, DstR4)         \
  BC_UNPACK_2xSLOT(Offset, DstR1, DstR2)                                       \
  BC_UNPACK_ZI64(Offset+BC_SLOT_SIZE*2, DstZ3)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*2 + 8, DstR4)

#define BC_UNPACK_2xSLOT_DICT_SLOT(Offset, DstR1, DstR2, DstR3, DstR4)         \
  BC_UNPACK_2xSLOT(Offset, DstR1, DstR2)                                       \
  BC_UNPACK_DICT(Offset+BC_SLOT_SIZE*2, DstR3)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*2 + BC_DICT_SIZE, DstR4)

#define BC_UNPACK_3xSLOT_RU16_SLOT(Offset, DstR1, DstR2, DstR3, DstR4, DstR5)  \
  BC_UNPACK_3xSLOT(Offset, DstR1, DstR2, DstR3)                                \
  BC_UNPACK_RU16(Offset+BC_SLOT_SIZE*3, DstR4)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*3 + 2, DstR5)

#define BC_UNPACK_3xSLOT_MASK_SLOT(Offset, DstR1, DstR2, DstR3, DstK4, DstR5)  \
  BC_UNPACK_3xSLOT(Offset, DstR1, DstR2, DstR3)                                \
  BC_UNPACK_MASK(Offset+BC_SLOT_SIZE*3, DstK4)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*3 + 2, DstR5)

#define BC_UNPACK_3xSLOT_ZF64_SLOT(Offset, DstR1, DstR2, DstR3, DstZ4, DstR5)  \
  BC_UNPACK_3xSLOT(Offset, DstR1, DstR2, DstR3)                                \
  BC_UNPACK_ZF64(Offset+BC_SLOT_SIZE*3, DstZ4)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*3 + 8, DstR5)

#define BC_UNPACK_3xSLOT_ZI64_SLOT(Offset, DstR1, DstR2, DstR3, DstZ4, DstR5)  \
  BC_UNPACK_3xSLOT(Offset, DstR1, DstR2, DstR3)                                \
  BC_UNPACK_ZI64(Offset+BC_SLOT_SIZE*3, DstZ4)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*3 + 8, DstR5)

#define BC_UNPACK_4xSLOT_RU16_SLOT(Offset, DstR1, DstR2, DstR3, DstR4, DstR5, DstR6) \
  BC_UNPACK_4xSLOT(Offset, DstR1, DstR2, DstR3, DstR4)                         \
  BC_UNPACK_RU16(Offset+BC_SLOT_SIZE*4, DstR5)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*4 + 2, DstR6)

#define BC_UNPACK_4xSLOT_MASK_SLOT(Offset, DstR1, DstR2, DstR3, DstR4, DstK5, DstR6) \
  BC_UNPACK_4xSLOT(Offset, DstR1, DstR2, DstR3, DstR4)                         \
  BC_UNPACK_MASK(Offset+BC_SLOT_SIZE*4, DstK5)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*4 + 2, DstR6)

#define BC_UNPACK_4xSLOT_ZF64_SLOT(Offset, DstR1, DstR2, DstR3, DstR4, DstZ5, DstR6) \
  BC_UNPACK_4xSLOT(Offset, DstR1, DstR2, DstR3, DstR4)                         \
  BC_UNPACK_ZF64(Offset+BC_SLOT_SIZE*4, DstZ5)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*4 + 8, DstR6)

#define BC_UNPACK_4xSLOT_ZI64_SLOT(Offset, DstR1, DstR2, DstR3, DstR4, DstZ5, DstR6) \
  BC_UNPACK_4xSLOT(Offset, DstR1, DstR2, DstR3, DstR4)                         \
  BC_UNPACK_ZI64(Offset+BC_SLOT_SIZE*4, DstZ5)                                 \
  BC_UNPACK_SLOT(Offset+BC_SLOT_SIZE*4 + 8, DstR6)

// BC Stack Load & Store Utilities
// -------------------------------

#define BC_LOAD_RU16_FROM_SLOT(Dst1, Slot)                                     \
  MOVWLZX 0(VIRT_VALUES)(Slot*1), Dst1

#define BC_LOAD_RU32_FROM_SLOT(Dst1, Slot)                                     \
  MOVL 0(VIRT_VALUES)(Slot*1), Dst1

#define BC_LOAD_RU64_FROM_SLOT(Dst1, Slot)                                     \
  MOVQ 0(VIRT_VALUES)(Slot*1), Dst1

#define BC_LOAD_K1_FROM_SLOT(Dst1, Slot)                                       \
  KMOVW 0(VIRT_VALUES)(Slot*1), Dst1

#define BC_LOAD_K1_K2_FROM_SLOT(DstK1, DstK2, Slot)                            \
  KMOVW 0(VIRT_VALUES)(Slot*1), DstK1                                          \
  KSHIFTRW $8, DstK1, DstK2

#define BC_LOAD_ZMM_FROM_SLOT(DstZ, Slot)                                      \
  VMOVDQU32 0(VIRT_VALUES)(Slot*1), DstZ

#define BC_LOAD_VALUE_SLICE_FROM_SLOT(DstZ1, DstZ2, Slot)                      \
  VMOVDQU32 0(VIRT_VALUES)(Slot*1), DstZ1                                      \
  VMOVDQU32 64(VIRT_VALUES)(Slot*1), DstZ2

#define BC_LOAD_VALUE_SLICE_FROM_SLOT_MASKED(DstZ1, DstZ2, Slot, Mask)         \
  VMOVDQU32.Z 0(VIRT_VALUES)(Slot*1), Mask, DstZ1                              \
  VMOVDQU32.Z 64(VIRT_VALUES)(Slot*1), Mask, DstZ2

#define BC_LOAD_VALUE_TYPEL_FROM_SLOT(DstZ, Slot)                              \
  VPMOVZXBD 128(VIRT_VALUES)(Slot*1), DstZ

#define BC_LOAD_VALUE_TYPEL_FROM_SLOT_MASKED(DstZ, Slot, Mask)                 \
  VPMOVZXBD.Z 128(VIRT_VALUES)(Slot*1), Mask, DstZ

#define BC_LOAD_VALUE_HLEN_FROM_SLOT(DstZ, Slot)                               \
  VPMOVZXBD 144(VIRT_VALUES)(Slot*1), DstZ

#define BC_LOAD_VALUE_HLEN_FROM_SLOT_MASKED(DstZ, Slot, Mask)                  \
  VPMOVZXBD.Z 144(VIRT_VALUES)(Slot*1), Mask, DstZ

#define BC_LOAD_SLICE_FROM_SLOT(DstZ1, DstZ2, Slot)                            \
  VMOVDQU32 0(VIRT_VALUES)(Slot*1), DstZ1                                      \
  VMOVDQU32 64(VIRT_VALUES)(Slot*1), DstZ2

#define BC_LOAD_SLICE_FROM_SLOT_MASKED(DstZ1, DstZ2, Slot, Mask)               \
  VMOVDQU32.Z 0(VIRT_VALUES)(Slot*1), Mask, DstZ1                              \
  VMOVDQU32.Z 64(VIRT_VALUES)(Slot*1), Mask, DstZ2

#define BC_LOAD_I64_FROM_SLOT(DstZ1, DstZ2, Slot)                              \
  VMOVDQU64 0(VIRT_VALUES)(Slot*1), DstZ1                                      \
  VMOVDQU64 64(VIRT_VALUES)(Slot*1), DstZ2

#define BC_LOAD_F64_FROM_SLOT(DstZ1, DstZ2, Slot)                              \
  VMOVUPD 0(VIRT_VALUES)(Slot*1), DstZ1                                        \
  VMOVUPD 64(VIRT_VALUES)(Slot*1), DstZ2

#define BC_LOAD_I64_FROM_SLOT_MASKED(DstZ1, DstZ2, Slot, Mask1, Mask2)         \
  VMOVDQU64.Z 0(VIRT_VALUES)(Slot*1), Mask1, DstZ1                             \
  VMOVDQU64.Z 64(VIRT_VALUES)(Slot*1), Mask2, DstZ2

#define BC_LOAD_F64_FROM_SLOT_MASKED(DstZ1, DstZ2, Slot, Mask1, Mask2)         \
  VMOVUPD.Z 0(VIRT_VALUES)(Slot*1), Mask1, DstZ1                               \
  VMOVUPD.Z 64(VIRT_VALUES)(Slot*1), Mask2, DstZ2

#define BC_STORE_RU16_TO_SLOT(Src, Slot)                                       \
  MOVW Src, 0(VIRT_VALUES)(Slot*1)

#define BC_STORE_RU32_TO_SLOT(Src, Slot)                                       \
  MOVL Src, 0(VIRT_VALUES)(Slot*1)

#define BC_STORE_K_TO_SLOT(SrcMask, Slot)                                      \
  KMOVW SrcMask, 0(VIRT_VALUES)(Slot*1)

#define BC_STORE_F64_TO_SLOT(Src1, Src2, Slot)                                 \
  VMOVUPD Src1, 0(VIRT_VALUES)(Slot*1)                                         \
  VMOVUPD Src2, 64(VIRT_VALUES)(Slot*1)

#define BC_STORE_I64_TO_SLOT(Src1, Src2, Slot)                                 \
  VMOVDQU64 Src1, 0(VIRT_VALUES)(Slot*1)                                       \
  VMOVDQU64 Src2, 64(VIRT_VALUES)(Slot*1)

#define BC_STORE_SLICE_TO_SLOT(Src1, Src2, Slot)                               \
  VMOVDQU64 Src1, 0(VIRT_VALUES)(Slot*1)                                       \
  VMOVDQU64 Src2, 64(VIRT_VALUES)(Slot*1)

#define BC_STORE_VALUE_TO_SLOT(SrcZ1, SrcZ2, SrcTlvZ, SrcHLenZ, Slot)          \
  VMOVDQU64 SrcZ1, 0(VIRT_VALUES)(Slot*1)                                      \
  VMOVDQU64 SrcZ2, 64(VIRT_VALUES)(Slot*1)                                     \
  VPMOVDB SrcTlvZ, 128(VIRT_VALUES)(Slot*1)                                    \
  VPMOVDB SrcHLenZ, 144(VIRT_VALUES)(Slot*1)

#define BC_STORE_VALUE_TO_SLOT_X(SrcZ1, SrcZ2, SrcTlvX, SrcHLenX, Slot)        \
  VMOVDQU64 SrcZ1, 0(VIRT_VALUES)(Slot*1)                                      \
  VMOVDQU64 SrcZ2, 64(VIRT_VALUES)(Slot*1)                                     \
  VMOVDQU8 SrcTlvX, 128(VIRT_VALUES)(Slot*1)                                   \
  VMOVDQU8 SrcHLenX, 144(VIRT_VALUES)(Slot*1)

// BC Working With Values and Symbols
// ----------------------------------

// Merges [Offset, Value] pairs into value offset and length registers.
#define BC_MERGE_VMREFS_TO_VALUE(ValOffZ, ValLenZ, LoZ, HiZ, Msk, TmpZ0, TmpY0, TmpY1) \
  VPMOVQD LoZ, TmpY0                                                            \
  VPSRLQ $32, LoZ, LoZ                                                          \
  VPMOVQD HiZ, TmpY1                                                            \
  VPSRLQ $32, HiZ, HiZ                                                          \
  VINSERTI32X8 $1, TmpY1, TmpZ0, Msk, ValOffZ                                   \
  VPMOVQD LoZ, TmpY0                                                            \
  VPMOVQD HiZ, TmpY1                                                            \
  VINSERTI32X8 $1, TmpY1, TmpZ0, Msk, ValLenZ

// This calculates TLV byte and HLen byte from a Value length. The purpose is to use
// this macro after unsymbolize to recreate these two bytes as they were invalidated
// by unsymbolizing. As we use a canonical representation it's possible to calculate
// header length from value length, and to recreate Type|L byte as we know that we
// have replaced symbols with strings.
//
// +------+----------+-------------------+-------------------+---------+
// | TLV  | [Length] | Value length      | String length     | Comment |
// +------+----------+-------------------+-------------------+---------+
// | 0x8L |   none   | 1-14              | 0-13              |         |
// | 0x8E | 1 byte   | 16-129            | 14-127            |         |
// | 0x8E | 2 bytes  | 131-16386         | 128-16383         |         |
// | 0x8E | 3 bytes  | 16388-2097155     | 16384-2097151     |         |
// | 0x8E | 4 bytes  | 2097157-268435460 | 2097152-268435455 | NotUsed |
// +------+----------+-------------------+-------------------+---------+
//
// LIMITS: Maximum VarUint decoded here is 3 bytes
#define BC_CALC_VALUE_HLEN(HLenZ, VLenZ, Msk, ConstD_1, ConstD_14, TmpZ1, TmpMsk)   \
  /* HLenZ <- updated header lengths to 1 (counting TLV byte) */                    \
  VMOVDQA32.Z ConstD_1, Msk, HLenZ                                                  \
  /* TmpMsk <- value length >= 16388 (at least 3-byte Length field) */              \
  VPCMPUD.BCST $VPCMP_IMM_GE, CONSTD_16388(), VLenZ, Msk, TmpMsk                    \
                                                                                    \
  /* HLenZ <- increase header length if the Length is encoded as 3 bytes or more */ \
  VPADDD ConstD_1, HLenZ, TmpMsk, HLenZ                                             \
  /* TmpMsk <- value length >= 131 (at least 2-byte Length field) */                \
  VPCMPUD.BCST $VPCMP_IMM_GE, CONSTD_131(), VLenZ, Msk, TmpMsk                      \
  /* TmpZ1 <- value length - 1 */                                                   \
  VPSUBD ConstD_1, VLenZ, TmpZ1                                                     \
  /* HLenZ <- increase header length if the Length is encoded as 2 bytes or more */ \
  VPADDD ConstD_1, HLenZ, TmpMsk, HLenZ                                             \
                                                                                    \
  /* TmpMsk <- (value length - 1) >= 14 (at least 1-byte Length field) */           \
  VPCMPUD $VPCMP_IMM_GE, ConstD_14, TmpZ1, Msk, TmpMsk                              \
  /* TmpZ1 <- min(value length - 1, 14) */                                          \
  VPMINUD ConstD_14, TmpZ1, TmpZ1                                                   \
  /* HLenZ <- increase header length if the Length is encoded as 1 byte or more */  \
  VPADDD ConstD_1, HLenZ, TmpMsk, HLenZ

#define BC_CALC_STRING_TLV_AND_HLEN(TlvZ, HLenZ, VLenZ, Msk, ConstD_1, ConstD_14, TmpZ1, TmpMsk) \
  /* TlvZ <- merged TLV byte with symbols replaced with strings (partial) */        \
  VPSLLD $7, ConstD_1, Msk, TlvZ                                                    \
  /* HLenZ <- updated header lengths to 1 (counting TLV byte) */                    \
  VMOVDQA32 ConstD_1, Msk, HLenZ                                                    \
  /* TmpMsk <- value length >= 16388 (at least 3-byte Length field) */              \
  VPCMPUD.BCST $VPCMP_IMM_GE, CONSTD_16388(), VLenZ, Msk, TmpMsk                    \
                                                                                    \
  /* HLenZ <- increase header length if the Length is encoded as 3 bytes or more */ \
  VPADDD ConstD_1, HLenZ, TmpMsk, HLenZ                                             \
  /* TmpMsk <- value length >= 131 (at least 2-byte Length field) */                \
  VPCMPUD.BCST $VPCMP_IMM_GE, CONSTD_131(), VLenZ, Msk, TmpMsk                      \
  /* TmpZ1 <- value length - 1 */                                                   \
  VPSUBD ConstD_1, VLenZ, TmpZ1                                                     \
  /* HLenZ <- increase header length if the Length is encoded as 2 bytes or more */ \
  VPADDD ConstD_1, HLenZ, TmpMsk, HLenZ                                             \
                                                                                    \
  /* TmpMsk <- (value length - 1) >= 14 (at least 1-byte Length field) */           \
  VPCMPUD $VPCMP_IMM_GE, ConstD_14, TmpZ1, Msk, TmpMsk                              \
  /* TmpZ1 <- min(value length - 1, 14) */                                          \
  VPMINUD ConstD_14, TmpZ1, TmpZ1                                                   \
  /* HLenZ <- increase header length if the Length is encoded as 1 byte or more */  \
  VPADDD ConstD_1, HLenZ, TmpMsk, HLenZ                                             \
  /* TlvZ <- merged TLV byte with symbols replaced with strings (final) */          \
  VPORD TmpZ1, TlvZ, Msk, TlvZ

// BC Scratch Buffer Allocation
// ----------------------------

// Inputs:
//   - SrcLen (ZMM) - Source lengths as UINT32 units, for each lane
//   - SrcMask (K)  - Source mask of all active lanes
//
// Outputs:
//   - DstSum (GP)  - Sum of all lengths, useful for final allocation (GP)
//   - DstOff (ZMM) - Start of each object in the output buffer relative to its current end (has to be further adjusted to get an absolute index)
//   - DstLen (ZMM) - Length of each object to be allocated (describes input lengths with large objects already masked out)
//   - DstEnd (ZMM) - Horizontally added lengths - it essentially contains the end of each object
//   - DstMask (K)  - Final predicate that contains lanes that were too large filtered out - this predicate MUST BE USED for allocation
#define BC_HORIZONTAL_LENGTH_SUM(DstSum, DstOff, DstLen, DstEnd, DstMask, SrcLen, SrcMask, TmpX1, TmpK1)                                                                             \
  /* Clear all lanes that would cause overflow during horizontal addition */                                                                                                         \
  VPCMPD.BCST $VPCMP_IMM_LE, CONSTD_134217727(), SrcLen, SrcMask, DstMask                                                                                                            \
  VMOVDQA32.Z SrcLen, DstMask, DstLen                              /* DstLen <- [15    14    13    12   |11    10    09    08   |07    06    05    04   |03    02    01    00   ] */ \
                                                                                                                                                                                     \
  MOVL $0xFF00F0F0, DstSum                                                                                                                                                           \
  VPSLLDQ $4, DstLen, DstEnd                                       /* DstEnd <- [14    13    12    __   |10    09    08    __   |06    05    04    __   |02    01    00    __   ] */ \
  VPADDD DstLen, DstEnd, DstEnd                                    /* DstEnd <- [15+14 14+13 13+12 12   |11+10 10+09 09+08 08   |07+06 06+05 05+04 04   |03+02 02+01 01+00 00   ] */ \
  VPSLLDQ $8, DstEnd, DstOff                                       /* DstOff <- [13+12 12    __    __   |09+08 08    __    __   |05+04 04    __    __   |01+00 00    __    __   ] */ \
  VPADDD DstOff, DstEnd, DstEnd                                    /* DstEnd <- [15:12 14:12 13:12 12   |11:08 10:08 09:08 08   |07:04 06:04 05:04 04   |03:00 02:00 01:00 00   ] */ \
                                                                                                                                                                                     \
  KMOVD DstSum, TmpK1                                                                                                                                                                \
  VPSHUFD $SHUFFLE_IMM_4x2b(3, 3, 3, 3), DstEnd, DstOff            /* DstOff <- [15:12 15:12 15:12 15:12|11:08 11:08 11:08 11:08|07:04 07:04 07:04 07:04|03:00 03:00 03:00 03:00] */ \
  VPERMQ $SHUFFLE_IMM_4x2b(1, 1, 1, 1), DstOff, DstOff             /* DstOff <- [11:08 11:08 11:08 11:08|<ign> <ign> <ign> <ign>|03:00 03:00 03:00 03:00|<ign> <ign> <ign> <ign>] */ \
  VPADDD DstOff, DstEnd, TmpK1, DstEnd                             /* DstEnd <- [15:08 14:08 13:08 12:08|11:08 10:08 09:08 08   |07:00 06:00 05:00 04:00|03:00 02:00 01:00 00   ] */ \
  KSHIFTRD $16, TmpK1, TmpK1                                                                                                                                                         \
  VPSHUFD $SHUFFLE_IMM_4x2b(3, 3, 3, 3), DstEnd, DstOff            /* DstOff <- [15:08 15:08 15:08 15:08|11:08 11:08 11:08 11:08|07:00 07:00 07:00 07:00|03:00 03:00 03:00 03:00] */ \
  VSHUFI64X2 $SHUFFLE_IMM_4x2b(1, 1, 1, 1), DstOff, DstOff, DstOff /* DstOff <- [07:00 07:00 07:00 07:00|07:00 07:00 07:00 07:00|<ign> <ign> <ign> <ign>|<ign> <ign> <ign> <ign>] */ \
  VPADDD DstOff, DstEnd, TmpK1, DstEnd                             /* DstEnd <- [15:00 14:00 13:00 12:00|11:00 10:00 09:00 08:00|07:00 06:00 05:00 04:00|03:00 02:00 01:00 00   ] */ \
                                                                                                                                                                                     \
  VEXTRACTI32X4 $3, DstEnd, TmpX1                                                                                                                                                    \
  VPSUBD DstLen, DstEnd, DstOff                                    /* DstOff <- [14:00 13:00 12:00 11:00|10:00 09:00 08:00 07:00|06:00 05:00 04:00 03:00|02:00 01:00 00    zero ] */ \
  VPEXTRD $3, TmpX1, DstSum                                        /* DstSum <- Aggregated length of all objects to be allocated */

#define BC_ALLOC_SLICE(DstOffZ, LenSumGP, TmpGP1, TmpGP2)                                                                \
  MOVQ bytecode_scratch+8(VIRT_BCPTR), TmpGP1                      /* TmpGP1 <- Scratch buffer length (start offset) */  \
  MOVQ bytecode_scratch+16(VIRT_BCPTR), TmpGP2                     /* TmpGP2 <- Scratch buffer capacity */               \
  SUBQ TmpGP1, TmpGP2                                              /* TmpGP2 <- Remaining space in the scratch buffer */ \
                                                                                                                         \
  CMPQ TmpGP2, LenSumGP                                            /* [NOT_ENOUGH_SCRATCH_SPACE] */                      \
  JLT error_handler_more_scratch                                   /* Abort if the scratch buffer is too small */        \
                                                                                                                         \
  VPBROADCASTD TmpGP1, DstOffZ                                     /* DstOffZ <- Initial offset */                       \
  VPADDD.BCST bytecode_scratchoff(VIRT_BCPTR), DstOffZ, DstOffZ    /* DstOffZ <- Initial offset + scratch offset */      \
  ADDQ LenSumGP, TmpGP1                                            /* TmpGP1 <- Updated scratch buffer length */         \
  MOVQ TmpGP1, bytecode_scratch+8(VIRT_BCPTR)                      /* [] <- Update the length of the scratch buffer */
