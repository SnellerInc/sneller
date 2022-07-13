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

// Package zion implements a "zipped" ion encoding
// that compresses streams of ion structures in a manner
// such that fields within structures in the stream
// can be decompressed without decompressing the entire
// input stream.
//
// Currently, the implementation of Encoder.Encode
// splits fields into buckets (by hashing)
// and then writes field contents into these buckets
// and compresses each bucket separately, along with
// a "shape" bitstream that is also compressed.
// Decoder.Decode decompresses the shape from the
// input bitstream and then uses a user-provided
// field selection to determine which buckets need
// to be decompressed as it walks the data.
//
// ## Shape Encoding Format
//
// The "shape" of a structure encodes just
// enough information to reconstruct a structure
// from the sixteen compressed buckets.
//
// Each structure is composed of one or more
// shape "sequences." Each sequence is composed
// of a 1-byte length+class descriptor followed by zero
// or more field descriptors. Each field descriptor
// is just a bucket number (from 0x0 to 0xf), so these
// are encoded as individual nibbles. The low 5 bits of
// the 1-byte length+class descriptor determine the
// number of fields that follow the descriptor (from 0 to 16 inclusive),
// and the top two bits of the descriptor encode a
// "size class" hint. (The size class hint is encoded
// as the number of bytes *minus one* that the ion structure descriptor
// would occupy. For example, a structure that was originally
// 8 bytes would have a descriptor 0xd8, so the size class descriptor
// would be 0. For 0xde1f it would be 1, and so forth; we do not
// support descriptors above 3.)
//
// For example, a structure with one field that
// lives in bucket 0xe would be encoded as:
//
//   0x01 0xe0
//
// A structure with four fields that live in
// buckets 0, 1, 2, 3:
//
//   0x04 0x01 0x23
//
// Notice that structures with odd field lengths
// still consume an integral number of bytes;
// the final (missing) field must be encoded as the 0 nibble.
// In other words, the length of the fields following
// a descriptor can be computed by:
//
//   class := shape[0]>>6            // size class
//   size := shape[0]&0x1f           // descriptor
//   body := shape[1:1+((size+1)/2)] // bytes of nibbles
//
// Since we can only record up to 16 fields in one sequence,
// a sequence of 16 fields does not terminate a structure,
// and the next sequence continues the fields where the
// previous one left off. (So, a structure with 16 fields
// will be composed of two sequences, where the second sequence
// is simply the 0x00 byte.)
//
// ## Decoding Process
//
// A "shape" stream composed of multiple structures
// *must* be decoded sequentially, since the shape
// stream itself only consists of bucket references.
//
// In order to unpack a structure, the decoder must
// consume the next ion label *and* ion value in
// each bucket that it steps through so that the
// bucket produces a new value each time it is
// referenced in the stream.
// (For example, the sequence of fields 0xffff would
// imply that bucket 15 would have to be decoded fifteen
// times in sequence).
package zion
