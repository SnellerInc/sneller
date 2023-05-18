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

// Code generated by ragel. DO NOT EDIT.

package jsonrl

import (
       "fmt"
       "unicode/utf8"
)

%%{
    machine datum;

    # one-character escape sequence:
    escape_sequence = (("\\" [tvfnrab\\\"/]) | ("\\u" xdigit{4})) > {esc = true};

    # multi-byte unicode sequences
    # (must be printable)
    # FIXME: perform rune calculation in-line; it is faster
    unicode_2c = (192 .. 223) . (128 .. 191) %{{
        r, size := utf8.DecodeRune(data[p-2:])
        if size != 2 {
           return fmt.Errorf("bad rune %x", r)
        }
    }};
    unicode_3c = (224 .. 239) . (128 .. 191) . (128 .. 191) %{{
        r, size := utf8.DecodeRune(data[p-3:])
        if size != 3 {
            return fmt.Errorf("bad rune %x", r)
        }
    }};
    unicode_4c = (240 .. 247) . (128 .. 191) . (128 .. 191) . (128 .. 191) %{{
        r, size := utf8.DecodeRune(data[p-4:])
        if size != 4 {
           return fmt.Errorf("bad rune %x", r)
        }
    }};

    unicode_sequence = unicode_2c | unicode_3c | unicode_4c;

    # one string character: printable or escape sequence
    string_chars = (ascii - [\\\"]) | escape_sequence | unicode_sequence;

    # quoted string: zero or more string charaters
    # (we capture the start and end offsets of the string)
    qstring = '"' %from{esc = false; sbegin = p;} (string_chars*) '"' >from{send = p};

    unsigned = digit+ ${curi *= 10; curi += uint64(data[p] - '0');};
    inttext = (('-' @{neg = true})? unsigned);

    # decimal part: continue parsing mantissa; track exponent
    decpart = digit* ${curi *= 10; curi += uint64(data[p] - '0'); dc--};
    # exponent part: parse exponent integer; add to decimal part
    epart = ((('-' @{nege = true})|'+')?) digit* ${cure *= 10; cure += int(data[p]) - '0';} %{
          if nege {
             cure = -cure
          }
          dc += cure
    };
    # fixme: if the input float string is long enough
    # that it could have overflowed the mantissa or exponent,
    # we should fall back to parsing the text using more precision
    # (for example, parsing "1.00000000000000011102230246251565404236316680908203126")
    dectext = (('-' @{neg = true})? unsigned) ('.' decpart)? ([eE] epart)?;

    # we cannot determine end-of-token
    # for e.g. integers until we see
    # a terminator:
    lbrace = '{' @{ t.tok = tokLBrace; };
    lbrack = '[' @{ t.tok = tokLBrack; };
}%%

%%{
    machine struct_field;
    include datum;
    # capture field name from quoted string:
    field_name = qstring @{
        field = data[sbegin:send];
        fieldesc = esc;
    };
    # try to lex the terminating character
    # so that we can avoid more calls to the lexer:
    end_comma = ',' @{ t.auxtok = tokComma; };
    end_rbrace = '}' @{ t.auxtok = tokRBrace; };
    terminator = space* (end_comma | end_rbrace);

    jsnull = "null" . terminator @{
        t.output.beginField(field, fieldesc);
        t.output.parseNull();
        goto ret;
    };
    jstrue = "true" . terminator @{
        t.output.beginField(field, fieldesc);
        t.output.parseBool(true);
        goto ret;
    };
    jsfalse = "false" . terminator @{
        t.output.beginField(field, fieldesc);
        t.output.parseBool(false);
        goto ret;
    };
    jsdec = dectext . terminator @{
        t.output.beginField(field, fieldesc);
        atof(t.output, curi, dc, neg);
        goto ret;
    };
    jsint = inttext . terminator @{{
        i := int64(curi)
        if neg { i = -i; }
        t.output.beginField(field, fieldesc);
        t.output.parseInt(i);
        goto ret;
    }};
    jsstr = qstring . terminator @{
        t.output.beginField(field, fieldesc);
        t.output.parseString(data[sbegin:send], esc);
        goto ret;
    };
    jsmore = (lbrace | lbrack) @{
        t.output.beginField(field, fieldesc);
        goto ret;
    };
    any_field = jsnull | jstrue | jsfalse | jsstr | jsint | jsdec | jsmore;
    field = field_name . space* . ':' . space* . any_field;
    end = '}' @{ t.tok = tokRBrace; goto ret; };
    # lex either
    #  "field" : datum
    # or the '}' token
    main := space* (field | end);
    write data;
}%%

func (t *parser) lexField(b *reader) error {
     if !b.assertFill() {
        if b.err != nil {
           return b.err
        }
        return fmt.Errorf("%w (unexpected EOF looking for struct field)", ErrNoMatch)
     }
     needmore := false
     for b.err == nil {
         t.tok = tokDatum
         t.auxtok = tokDatum
         neg, nege, esc, fieldesc := false, false, false, false
         sbegin, send := 0, 0
         curi, cure, dc := uint64(0), int(0), int(0)
         var field []byte
         data := b.avail()
         cs, p, pe, eof := 0, 0, len(data), len(data)
         _ = eof
         %%{
            write init;
            write exec;
        }%%
        goto no
ret:    // jumped to from body:
        if t.tok == tokRBrace && needmore {
            return fmt.Errorf("%w: rejecting ',' before '}'", ErrNoMatch)
        }
        b.rpos += p+1
        // if we see ',' then keep looping
        if t.tok == tokDatum && t.auxtok == tokComma {
            err := t.output.out.CheckSize()
            if err != nil {
                return err
            }
            needmore = true
            continue
        }
        return nil
no:     // fallthrough path
        if p < len(data) {
           return fmt.Errorf("lexing struct field: %w", ErrNoMatch)
        }
        if len(data) >= MaxDatumSize {
           if field != nil {
              return fmt.Errorf("field %q: %w", field, ErrTooLarge)
           }
           return fmt.Errorf("struct field: %w", ErrTooLarge)
        }
        if b.atEOF {
           return fmt.Errorf("%w (unexpected EOF)", ErrNoMatch)
        }
        b.fill()
     }
     return b.err
}

%%{
    machine struct_cont;
    more = ',' @{ t.tok = tokComma; };
    end = '}' @{ t.tok = tokRBrace; };
    main := space* . (more | end) @{ b.rpos += p+1; return nil; };
    write data;
}%%

func (t *parser) lexMoreStruct(b *reader) error {
     if !b.assertFill() {
        if b.err != nil {
           return b.err
        }
        return fmt.Errorf("%w (unexpected EOF looking for struct field)", ErrNoMatch)
     }
     for b.err == nil {
         data := b.avail()
         cs, p, pe, eof := 0, 0, len(data), len(data)
         _ = eof
         %%{
            write init;
            write exec;
        }%%
        if p < len(data) {
           return fmt.Errorf("%w (couldn't find ',' or '}' in struct)", ErrNoMatch)
        }
        if len(data) >= MaxDatumSize {
           return fmt.Errorf("struct whitespace: %w", ErrTooLarge)
        }
        if b.atEOF {
            return fmt.Errorf("%w (unexpected EOF looking ',' or '}')", ErrNoMatch)
        }
        b.fill()
     }
     return b.err
}

%%{
    machine list_field;
    include datum;
    # try to lex the terminating character
    # so that we can avoid more calls to the lexer:
    end_comma = ',' @{ t.auxtok = tokComma; };
    end_rbrack = ']' @{ t.auxtok = tokRBrack; };
    terminator = space* (end_comma | end_rbrack);

    jsnull = "null" . terminator @{
        t.output.parseNull();
        goto ret;
    };
    jstrue = "true" . terminator @{
        t.output.parseBool(true);
        goto ret;
    };
    jsfalse = "false" . terminator @{
        t.output.parseBool(false);
        goto ret;
    };
    jsdec = dectext . terminator @{
        atof(t.output, curi, dc, neg);
        goto ret;
    };
    jsint = inttext . terminator @{{
        i := int64(curi)
        if neg { i = -i; }
        t.output.parseInt(i);
        goto ret;
    }};
    jsstr = qstring . terminator @{
        t.output.parseString(data[sbegin:send], esc);
        goto ret;
    };
    jsmore = (lbrace | lbrack) @{ goto ret; };
    any_field = jsnull | jstrue | jsfalse | jsstr | jsint | jsdec | jsmore;
    end = ']' @{ t.tok = tokRBrack; b.rpos += p+1; return nil };
    main := space* . (any_field | end);
    write data;
}%%

func (t *parser) lexListField(b *reader, multi bool) error {
     if !b.assertFill() {
        if b.err != nil {
           return b.err
        }
        return fmt.Errorf("%w (unexpected EOF looking for struct field)", ErrNoMatch)
     }
     needmore := false
     for b.err == nil {
         t.tok = tokDatum
         t.auxtok = tokDatum
         neg, nege, esc := false, false, false
         sbegin, send := 0, 0
         curi, cure, dc := uint64(0), int(0), int(0)
         data := b.avail()
         cs, p, pe, eof := 0, 0, len(data), len(data)
         _ = eof
         %%{
            write init;
            write exec;
        }%%
        goto no
ret:    // jumped to from lexer
        {
            // assert we didn't get ",]"
            if t.tok == tokRBrack && needmore {
                return fmt.Errorf("%w: rejecting ',' before ']'", ErrNoMatch)
            }
            b.rpos += p+1
            // loop if we saw ',' as our terminator
            if multi && t.tok == tokDatum && t.auxtok == tokComma {
                err := t.output.out.CheckSize()
                if err != nil {
                   return err
                }
                needmore = true
                continue
            }
            return nil
        }
no:
        if p < len(data) {
            return fmt.Errorf("%w couldn't find ']' or datum in list", ErrNoMatch)
        }
        if len(data) >= MaxDatumSize {
           return fmt.Errorf("list item: %w", ErrTooLarge)
        }
        if b.atEOF {
           return fmt.Errorf("%w (unexpected EOF looking for datum or ']')", ErrNoMatch)
        }
        b.fill()
     }
     return b.err
}

%%{
    machine list_cont;
    more = ',' @{ t.tok = tokComma; };
    end = ']' @{ t.tok = tokRBrack; };
    main := space* . (more | end) @{ b.rpos += p+1; return nil; };
    write data;
}%%

// lex either tokComma or tokRBrack
func (t *parser) lexMoreList(b *reader) error {
     if !b.assertFill() {
        if b.err != nil {
           return b.err
        }
        return fmt.Errorf("%w (unexpected EOF looking for struct field)", ErrNoMatch)
     }
     for b.err == nil {
         t.tok = tokEOF
         data := b.avail()
         cs, p, pe, eof := 0, 0, len(data), len(data)
         _ = eof
         %%{
            write init;
            write exec;
        }%%
        if p < len(data) {
            return fmt.Errorf("%w (couldn't find ',' or ']' in list)", ErrNoMatch)
        }
        if len(data) >= MaxDatumSize {
            return fmt.Errorf("list whitespace: %w", ErrTooLarge)
        }
        if b.atEOF {
           return fmt.Errorf("%w (unexpected EOF looking for ',' or ']')", ErrNoMatch)
        }
        b.fill()
     }
     return b.err
}

%%{
    machine toplevel;
    include datum;
    main := space* (lbrace | lbrack) @{ b.rpos += p+1; return nil; };
    write data;
}%%

func (t *parser) lexToplevel(b *reader) error {
     b.fill()
     for b.err == nil {
         t.tok = tokEOF
         if b.atEOF && b.buffered() == 0 {
            return nil
         }
         data := b.avail()
         cs, p, pe, eof := 0, 0, len(data), len(data)
         _ = eof
         %%{
            write init;
            write exec;
        }%%
        if p < len(data) {
            return fmt.Errorf("%w (couldn't find '[' or '{'", ErrNoMatch)
        }
        if b.atEOF {
            // everything was whitespace and we are at EOF
            return nil
        }
        if len(data) >= MaxDatumSize {
            return fmt.Errorf("top-level whitespace: %w", ErrTooLarge)
        }
        b.fill()
     }
     return b.err
}
