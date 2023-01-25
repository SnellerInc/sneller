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
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/ion"
)

// testFindSymSingleValue is a findsym tester that tests matching a single
// Symbol/Value pair. It can be used to verify many Symbol+Value encoding
// variations.
func testFindSymSingleValue(t *testing.T, st *ion.Symtab, symbolID ion.Symbol, values []any) {
	var ctx bctestContext
	defer ctx.free()

	symLen := uint32(1)

	if symbolID >= 1<<7 {
		symLen = 2
	}

	if symbolID >= 1<<14 {
		symLen = 3
	}

	inputB := ctx.bRegFromValues(values, st)
	inputK := kRegData{mask: uint16(uint64(1)<<len(values) - 1)}

	outputV := vRegData{}
	outputK := kRegData{}

	if err := ctx.executeOpcode(opfindsym, []any{&outputV, &outputK, &inputB, symbolID, &inputK}, inputK); err != nil {
		t.Fatal(err)
	}

	expectedOutputV := vRegData{}
	for i := 0; i < bcLaneCount; i++ {
		if inputB.sizes[i] != 0 {
			expectedOutputV.offsets[i] = inputB.offsets[i] + symLen
			expectedOutputV.sizes[i] = inputB.sizes[i] - symLen
		}
	}

	verifyKRegOutput(t, &outputK, &inputK)
	verifyVRegOutputP(t, &outputV, &expectedOutputV, &outputK)
}

func addRandomSymbolsToSymtab(st *ion.Symtab, count int) {
	r := rand.NewSource(time.Now().UnixNano())
	l := 32
	for i := 0; i < count; i++ {
		content := make([]byte, l)
		for j := 0; j < l; j++ {
			content[j] = byte(32 + (r.Int63() & 0x3F))
		}
		st.InternBytes(content)
	}
}

func testFindSymSingleValue0ByteLength(t *testing.T, st *ion.Symtab, symID ion.Symbol) {
	sym := st.Get(symID)
	testFindSymSingleValue(t, st, symID, []any{
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Null}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Bool(false)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Bool(true)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(0)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(128)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(256)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(1024)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(10000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(100000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(1000000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(10000000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(100000000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(1000000000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(10000000000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Float(0.5)}}),
	})
}

func testFindSymSingleValue1ByteLength(t *testing.T, st *ion.Symtab, symID ion.Symbol) {
	sym := st.Get(symID)
	testFindSymSingleValue(t, st, symID, []any{
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Null}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Bool(false)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Bool(true)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.String("this is a short string that doesn't fit into a single L")}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(0)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(128)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(256)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(1024)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(10000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(100000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(1000000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(10000000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(100000000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.String("nested string")}}).Datum()}}),
	})
}

func testFindSymSingleValue2ByteLength(t *testing.T, st *ion.Symtab, symID ion.Symbol) {
	shortString := "this is a short string that doesn't fit into a single L"
	longString := strings.Repeat("-", 128)

	sym := st.Get(symID)
	testFindSymSingleValue(t, st, symID, []any{
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Null}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.String(shortString)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.String(longString)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(0)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(128)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(256)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(1024)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(10000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(100000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(1000000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(10000000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(100000000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.String(shortString)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.String(longString + "ABCDEFGH")}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.String("nested string")}}).Datum()}}),
	})
}

func testFindSymSingleValue3ByteLength(t *testing.T, st *ion.Symtab, symID ion.Symbol) {
	shortString := "this is a short string that doesn't fit into a single L"
	longString := strings.Repeat("-", 199)
	longestString := strings.Repeat("=", 17985)

	sym := st.Get(symID)
	testFindSymSingleValue(t, st, symID, []any{
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Null}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.String(shortString)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.String(longString)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.String(longestString)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(0)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(128)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(256)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(1024)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(10000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(100000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.Int(1000000)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.String(shortString)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.String(longString)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.String(longestString)}}),
		ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.NewStruct(st, []ion.Field{{Label: sym, Datum: ion.String("nested string")}}).Datum()}}),
	})
}

func TestFindSym1ByteSymbol0ByteLength(t *testing.T) {
	t.Parallel()

	st := ion.Symtab{}
	testFindSymSingleValue0ByteLength(t, &st, st.Intern("a"))
}

func TestFindSym1ByteSymbol1ByteLength(t *testing.T) {
	t.Parallel()

	st := ion.Symtab{}
	testFindSymSingleValue1ByteLength(t, &st, st.Intern("a"))
}

func TestFindSym1ByteSymbol2ByteLength(t *testing.T) {
	t.Parallel()

	st := ion.Symtab{}
	testFindSymSingleValue2ByteLength(t, &st, st.Intern("a"))
}

func TestFindSym1ByteSymbol3ByteLength(t *testing.T) {
	t.Parallel()

	st := ion.Symtab{}
	testFindSymSingleValue3ByteLength(t, &st, st.Intern("a"))
}

func TestFindSym2ByteSymbol0ByteLength(t *testing.T) {
	t.Parallel()

	st := ion.Symtab{}
	addRandomSymbolsToSymtab(&st, 133)
	testFindSymSingleValue0ByteLength(t, &st, st.Intern("a"))
}

func TestFindSym2ByteSymbol1ByteLength(t *testing.T) {
	t.Parallel()

	st := ion.Symtab{}
	addRandomSymbolsToSymtab(&st, 133)
	testFindSymSingleValue1ByteLength(t, &st, st.Intern("a"))
}

func TestFindSym2ByteSymbol2ByteLength(t *testing.T) {
	t.Parallel()

	st := ion.Symtab{}
	addRandomSymbolsToSymtab(&st, 133)
	testFindSymSingleValue2ByteLength(t, &st, st.Intern("a"))
}

func TestFindSym2ByteSymbol3ByteLength(t *testing.T) {
	t.Parallel()

	st := ion.Symtab{}
	addRandomSymbolsToSymtab(&st, 133)
	testFindSymSingleValue3ByteLength(t, &st, st.Intern("a"))
}

func TestFindSym3ByteSymbol0ByteLength(t *testing.T) {
	t.Parallel()

	st := ion.Symtab{}
	addRandomSymbolsToSymtab(&st, 17775)
	testFindSymSingleValue0ByteLength(t, &st, st.Intern("a"))
}

func TestFindSym3ByteSymbol1ByteLength(t *testing.T) {
	t.Parallel()

	st := ion.Symtab{}
	addRandomSymbolsToSymtab(&st, 17775)
	testFindSymSingleValue1ByteLength(t, &st, st.Intern("a"))
}

func TestFindSym3ByteSymbol2ByteLength(t *testing.T) {
	t.Parallel()

	st := ion.Symtab{}
	addRandomSymbolsToSymtab(&st, 17775)
	testFindSymSingleValue2ByteLength(t, &st, st.Intern("a"))
}

func TestFindSym3ByteSymbol3ByteLength(t *testing.T) {
	t.Parallel()

	st := ion.Symtab{}
	addRandomSymbolsToSymtab(&st, 17775)
	testFindSymSingleValue3ByteLength(t, &st, st.Intern("a"))
}

func TestFindSym2NoMissing(t *testing.T) {
	t.Parallel()

	var ctx bctestContext
	defer ctx.free()

	st := ion.Symtab{}
	aSymID := st.Intern("a")
	bSymID := st.Intern("b")

	if aSymID >= bSymID {
		t.Errorf("The first symbol (%d) must be lesser than the second symbol (%d)", aSymID, bSymID)
		t.FailNow()
	}

	// The size of each "a" key/value is i + 2 bytes (1 byte symbol, 1 byte string header, the rest is content).
	inputB := ctx.bRegFromValues([]any{
		ion.NewStruct(&st, []ion.Field{
			{Label: "a", Datum: ion.String("-")},
			{Label: "b", Datum: ion.String("a|")},
		}),
		ion.NewStruct(&st, []ion.Field{
			{Label: "a", Datum: ion.String("--")},
			{Label: "b", Datum: ion.String("ab|")},
		}),
		ion.NewStruct(&st, []ion.Field{
			{Label: "a", Datum: ion.String("---")},
			{Label: "b", Datum: ion.String("abc|")},
		}),
		ion.NewStruct(&st, []ion.Field{
			{Label: "a", Datum: ion.String("----")},
			{Label: "b", Datum: ion.String("abcd|")},
		}),
		ion.NewStruct(&st, []ion.Field{
			{Label: "a", Datum: ion.String("-----")},
			{Label: "b", Datum: ion.String("abcde|")},
		}),
		ion.NewStruct(&st, []ion.Field{
			{Label: "a", Datum: ion.String("------")},
			{Label: "b", Datum: ion.String("abcdef|")},
		}),
		ion.NewStruct(&st, []ion.Field{
			{Label: "a", Datum: ion.String("-------")},
			{Label: "b", Datum: ion.String("abcdefg|")},
		}),
		ion.NewStruct(&st, []ion.Field{
			{Label: "a", Datum: ion.String("--------")},
			{Label: "b", Datum: ion.String("abcdefgh|")},
		}),
		ion.NewStruct(&st, []ion.Field{
			{Label: "a", Datum: ion.String("---------")},
			{Label: "b", Datum: ion.String("abcdefghi|")},
		}),
	}, &st)
	inputK := kRegData{mask: uint16(0xFF)}

	// First test whether findsym finds "a"
	outputV1 := vRegData{}
	outputK1 := kRegData{}

	if err := ctx.executeOpcode(opfindsym, []any{&outputV1, &outputK1, &inputB, aSymID, &inputK}, inputK); err != nil {
		t.Fatal(err)
	}

	expectedOutputV1 := vRegData{}
	for i := 0; i < bcLaneCount; i++ {
		if inputB.sizes[i] != 0 {
			expectedOutputV1.offsets[i] = inputB.offsets[i] + 1
			expectedOutputV1.sizes[i] = uint32(i + 2)
		}
	}
	verifyKRegOutput(t, &outputK1, &inputK)
	verifyVRegOutputP(t, &outputV1, &expectedOutputV1, &outputK1)

	// Now test whether both findsym and findsym2 find "b"
	outputV2 := vRegData{}
	outputK2 := kRegData{}

	if err := ctx.executeOpcode(opfindsym2, []any{&outputV2, &outputK2, &inputB, &outputV1, &outputK1, bSymID, &inputK}, inputK); err != nil {
		t.Fatal(err)
	}

	if err := ctx.executeOpcode(opfindsym, []any{&outputV1, &outputK1, &inputB, bSymID, &inputK}, inputK); err != nil {
		t.Fatal(err)
	}

	verifyKRegOutput(t, &outputK2, &outputK1)
	verifyVRegOutput(t, &outputV2, &outputV1)
}

func TestFindSym2WithMissing(t *testing.T) {
	t.Parallel()

	var ctx bctestContext
	defer ctx.free()

	st := ion.Symtab{}
	aSymID := st.Intern("a")
	bSymID := st.Intern("b")

	if aSymID >= bSymID {
		t.Errorf("The first symbol (%d) must be lesser than the second symbol (%d)", aSymID, bSymID)
		t.FailNow()
	}

	// The size of each "a" key/value is i + 2 bytes (1 byte symbol, 1 byte string header, the rest is content).
	inputB := ctx.bRegFromValues([]any{
		ion.NewStruct(&st, []ion.Field{
			{Label: "a", Datum: ion.String("-")},
		}),
		ion.NewStruct(&st, []ion.Field{
			{Label: "b", Datum: ion.String("ab|")},
		}),
		ion.NewStruct(&st, []ion.Field{
			{Label: "a", Datum: ion.String("---")},
			{Label: "b", Datum: ion.String("abc|")},
		}),
		ion.NewStruct(&st, []ion.Field{
			{Label: "a", Datum: ion.String("-----")},
			{Label: "b", Datum: ion.String("abcde|")},
		}),
		ion.NewStruct(&st, []ion.Field{
			{Label: "a", Datum: ion.String("------")},
			{Label: "b", Datum: ion.String("abcdef|")},
		}),
		ion.NewStruct(&st, []ion.Field{
			{Label: "a", Datum: ion.String("-------")},
		}),
		ion.NewStruct(&st, []ion.Field{
			{Label: "b", Datum: ion.String("abcdefgh|")},
		}),
		ion.NewStruct(&st, []ion.Field{
			{Label: "a", Datum: ion.String("---------")},
			{Label: "b", Datum: ion.String("abcdefghi|")},
		}),
		ion.NewStruct(&st, []ion.Field{}),
		ion.NewStruct(&st, []ion.Field{}),
		ion.NewStruct(&st, []ion.Field{}),
		ion.NewStruct(&st, []ion.Field{}),
	}, &st)
	inputK := kRegData{mask: uint16(0xFF)}

	outputV1 := vRegData{}
	outputK1 := kRegData{}

	outputV2 := vRegData{}
	outputK2 := kRegData{}

	if err := ctx.executeOpcode(opfindsym, []any{&outputV1, &outputK1, &inputB, bSymID, &inputK}, inputK); err != nil {
		t.Fatal(err)
	}

	if err := ctx.executeOpcode(opfindsym, []any{&outputV2, &outputK2, &inputB, aSymID, &inputK}, inputK); err != nil {
		t.Fatal(err)
	}

	if err := ctx.executeOpcode(opfindsym2, []any{&outputV2, &outputK2, &inputB, &outputV2, &outputK2, bSymID, &inputK}, inputK); err != nil {
		t.Fatal(err)
	}

	verifyKRegOutput(t, &outputK2, &outputK1)
	verifyVRegOutput(t, &outputV2, &outputV1)
}
