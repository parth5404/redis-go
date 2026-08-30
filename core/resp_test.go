package core

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// RESP parser tests.
//
// The original core/resp_test.go was 100% commented out, so none of the parser's
// behaviour was pinned down. These tests exist mainly as regression coverage for
// input that used to crash the process: every entry in the malformed table below
// either panicked, over-read the buffer, or allocated without bound before the
// rewrite.

func TestDecodeSimpleString(t *testing.T) {
	v, n, err := DecodeOne([]byte("+OK\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != "OK" || n != 5 {
		t.Fatalf("got (%v, %d), want (OK, 5)", v, n)
	}
}

func TestDecodeInteger(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{":0\r\n", 0},
		{":42\r\n", 42},
		{":-42\r\n", -42},
		{":9223372036854775807\r\n", 9223372036854775807},
		{":-9223372036854775808\r\n", -9223372036854775808},
	} {
		v, _, err := DecodeOne([]byte(tc.in))
		if err != nil {
			t.Fatalf("%q: unexpected error: %v", tc.in, err)
		}
		if v != tc.want {
			t.Errorf("%q: got %v, want %d", tc.in, v, tc.want)
		}
	}
}

func TestDecodeBulkString(t *testing.T) {
	v, n, err := DecodeOne([]byte("$5\r\nhello\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != "hello" || n != 11 {
		t.Fatalf("got (%v, %d), want (hello, 11)", v, n)
	}
}

// TestDecodeEmptyVersusNullBulkString pins the distinction the whole
// key-does-not-exist reply depends on.
func TestDecodeEmptyVersusNullBulkString(t *testing.T) {
	v, _, err := DecodeOne([]byte("$0\r\n\r\n"))
	if err != nil {
		t.Fatalf("empty bulk: %v", err)
	}
	if v != "" {
		t.Errorf("empty bulk decoded to %#v, want the empty string", v)
	}

	v, _, err = DecodeOne([]byte("$-1\r\n"))
	if err != nil {
		t.Fatalf("null bulk: %v", err)
	}
	if v != nil {
		t.Errorf("null bulk decoded to %#v, want nil", v)
	}
}

// TestDecodeBulkStringWithEmbeddedCRLF checks that the length prefix is trusted
// over any scan for a delimiter. A value containing \r\n must survive intact --
// this is the property a space- or newline-delimited format cannot provide, and
// is why the AOF now uses RESP framing.
func TestDecodeBulkStringWithEmbeddedCRLF(t *testing.T) {
	payload := "line1\r\nline2"
	frame := "$" + itoa(len(payload)) + "\r\n" + payload + "\r\n"
	v, n, err := DecodeOne([]byte(frame))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != payload {
		t.Errorf("got %q, want %q", v, payload)
	}
	if n != len(frame) {
		t.Errorf("consumed %d bytes, want %d", n, len(frame))
	}
}

func TestDecodeArray(t *testing.T) {
	v, _, err := DecodeOne([]byte("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arr, ok := v.([]interface{})
	if !ok {
		t.Fatalf("got %T, want []interface{}", v)
	}
	if len(arr) != 2 || arr[0] != "GET" || arr[1] != "foo" {
		t.Fatalf("got %#v", arr)
	}
}

func TestDecodeNestedArray(t *testing.T) {
	v, _, err := DecodeOne([]byte("*2\r\n*1\r\n$1\r\na\r\n:7\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arr := v.([]interface{})
	inner, ok := arr[0].([]interface{})
	if !ok || len(inner) != 1 || inner[0] != "a" {
		t.Fatalf("inner array wrong: %#v", arr[0])
	}
	if arr[1] != int64(7) {
		t.Fatalf("second element %#v, want int64(7)", arr[1])
	}
}

// TestDecodeIncompleteFrames is the property that makes partial reads work:
// a valid prefix must report ErrIncomplete, never a protocol error, because the
// caller answers the two cases differently -- wait for more bytes versus close
// the connection.
func TestDecodeIncompleteFrames(t *testing.T) {
	prefixes := []string{
		"",
		"*",
		"*1",
		"*1\r",
		"*1\r\n",
		"*1\r\n$",
		"*1\r\n$3",
		"*1\r\n$3\r\n",
		"*1\r\n$3\r\nGE",
		"*1\r\n$3\r\nGET",
		"*1\r\n$3\r\nGET\r",
		"+PART",
		":12",
		"$5\r\nhel",
		"*2\r\n$3\r\nGET\r\n$3\r\nfo",
		"PING", // inline, no CRLF yet

		// The two payloads that used to crash the process. A declared bulk
		// length larger than what has arrived is *incomplete*, not malformed:
		// the remaining bytes may still be in flight. What must never happen is
		// slicing past the data -- which, because Go re-slices up to a slice's
		// capacity, either returned unrelated buffer bytes or panicked.
		"*1\r\n$100\r\nab",
		"*1\r\n$5000\r\nab",
	}
	for _, p := range prefixes {
		_, _, err := DecodeOne([]byte(p))
		if !errors.Is(err, ErrIncomplete) {
			t.Errorf("DecodeOne(%q) = %v, want ErrIncomplete", p, err)
		}
	}
}

// TestDecodeIncrementalDelivery feeds a command one byte at a time, which is
// what a TCP stream is entitled to do. Only the final byte may yield a command.
func TestDecodeIncrementalDelivery(t *testing.T) {
	full := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	for i := 1; i < len(full); i++ {
		if _, _, err := DecodeOne([]byte(full[:i])); !errors.Is(err, ErrIncomplete) {
			t.Fatalf("prefix of length %d = %v, want ErrIncomplete", i, err)
		}
	}
	v, n, err := DecodeOne([]byte(full))
	if err != nil {
		t.Fatalf("full frame: %v", err)
	}
	if n != len(full) {
		t.Fatalf("consumed %d, want %d", n, len(full))
	}
	if len(v.([]interface{})) != 3 {
		t.Fatalf("got %#v", v)
	}
}

// TestDecodeMalformedDoesNotPanic is the regression suite for the crash bugs.
//
// Each of these inputs is a complete, unambiguous protocol violation and must
// produce an error. The subtest form matters: a panic in one case still lets the
// others report, and names the exact input in the failure.
func TestDecodeMalformedDoesNotPanic(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"bulk length negative", "$-5\r\nabc\r\n"},
		{"bulk length not a number", "$abc\r\nxx\r\n"},
		{"bulk missing trailing CRLF", "$2\r\nabxx"},
		{"bulk length absurdly large", "$999999999999999999999\r\n"},

		// OOM amplification: a 12-byte request that asked for 16 GB of
		// interface headers.
		{"multibulk count absurd", "*999999999\r\n"},
		{"multibulk count negative", "*-5\r\n"},
		{"multibulk count not a number", "*abc\r\n"},

		{"bare integer marker", ":\r\n"},
		{"integer not a number", ":abc\r\n"},
		{"integer overflows int64", ":99999999999999999999\r\n"},

		{"control byte", "\x01\x02\x03\r\n"},
		{"unbalanced double quote", "SET k \"unterminated\r\n"},
		{"unbalanced single quote", "SET k 'unterminated\r\n"},

		// Stack exhaustion by nesting.
		{"nesting too deep", strings.Repeat("*1\r\n", 64) + "$1\r\na\r\n"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked on %q: %v", tc.in, r)
				}
			}()
			_, _, err := DecodeOne([]byte(tc.in))
			if err == nil {
				t.Fatalf("DecodeOne(%q) succeeded, want an error", tc.in)
			}
			if errors.Is(err, ErrIncomplete) {
				t.Fatalf("DecodeOne(%q) = ErrIncomplete, want a protocol error: "+
					"treating this as 'read more' would let the client stall the connection", tc.in)
			}
		})
	}
}

// TestDecodeOverReadIsNotSilent is the sharper form of the crash test. The
// original returned bytes from *later in the same buffer* for this input rather
// than failing, which is an information disclosure: one client could read
// another's data out of a recycled buffer.
func TestDecodeOverReadIsNotSilent(t *testing.T) {
	// A buffer with spare capacity, exactly as a socket read produces.
	buf := make([]byte, 0, 512)
	buf = append(buf, "*1\r\n$100\r\nab"...)
	// Fill the rest of the capacity with a marker a leak would expose.
	full := buf[:cap(buf)]
	for i := len(buf); i < len(full); i++ {
		full[i] = 'S'
	}

	v, _, err := DecodeOne(buf)
	if err == nil {
		t.Fatalf("over-read succeeded and returned %#v; the declared length "+
			"(100) exceeds the %d bytes actually received", v, len(buf))
	}
	if !errors.Is(err, ErrIncomplete) {
		t.Fatalf("got %v, want ErrIncomplete (100 bytes may still arrive)", err)
	}
}

func TestDecodeInlineCommands(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"PING\r\n", []string{"PING"}},
		{"SET foo bar\r\n", []string{"SET", "foo", "bar"}},
		{"SET   foo   bar\r\n", []string{"SET", "foo", "bar"}},
		{"SET foo \"two words\"\r\n", []string{"SET", "foo", "two words"}},
		{"SET foo 'two words'\r\n", []string{"SET", "foo", "two words"}},
		{"SET foo \"a\\nb\"\r\n", []string{"SET", "foo", "a\nb"}},
	}
	for _, tc := range cases {
		v, _, err := DecodeOne([]byte(tc.in))
		if err != nil {
			t.Fatalf("%q: %v", tc.in, err)
		}
		got, err := DecodeArrayString(v.([]interface{}))
		if err != nil {
			t.Fatalf("%q: %v", tc.in, err)
		}
		if len(got) != len(tc.want) {
			t.Fatalf("%q: got %#v, want %#v", tc.in, got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%q: arg %d = %q, want %q", tc.in, i, got[i], tc.want[i])
			}
		}
	}
}

// TestDecodePipeline checks that several commands in one buffer all decode, with
// byte counts that let the caller advance correctly.
func TestDecodePipeline(t *testing.T) {
	stream := "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
	values, err := Decode([]byte(stream))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(values) != 3 {
		t.Fatalf("decoded %d values, want 3", len(values))
	}
}

// TestDecodeTolerateTruncatedTail is what makes a half-written AOF loadable:
// every whole record before the truncation must be returned.
func TestDecodeTolerateTruncatedTail(t *testing.T) {
	stream := "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\n"
	values, err := Decode([]byte(stream))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(values) != 1 {
		t.Fatalf("decoded %d values, want 1 (the complete prefix)", len(values))
	}
}

// TestDecodeArrayStringConvertsNonStrings pins the fix for the unchecked
// `v.(string)` assertion that panicked on an integer argument.
func TestDecodeArrayStringConvertsNonStrings(t *testing.T) {
	v, _, err := DecodeOne([]byte("*2\r\n$3\r\nGET\r\n:5\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, err := DecodeArrayString(v.([]interface{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got[0] != "GET" || got[1] != "5" {
		t.Fatalf("got %#v, want [GET 5]", got)
	}
}

func TestDecodeEmptyArrayIsNotAnError(t *testing.T) {
	v, n, err := DecodeOne([]byte("*0\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 4 {
		t.Fatalf("consumed %d, want 4", n)
	}
	arr, ok := v.([]interface{})
	if !ok || len(arr) != 0 {
		t.Fatalf("got %#v, want an empty array", v)
	}
}

// --- encoders ---------------------------------------------------------------

func TestEncoders(t *testing.T) {
	cases := []struct {
		got  []byte
		want string
	}{
		{EncodeSimpleString("OK"), "+OK\r\n"},
		{EncodeError(errors.New("ERR nope")), "-ERR nope\r\n"},
		{EncodeInt(-3), ":-3\r\n"},
		{EncodeBulkString(""), "$0\r\n\r\n"},
		{EncodeBulkString("hi"), "$2\r\nhi\r\n"},
		{EncodeBulkString("a\r\nb"), "$4\r\na\r\nb\r\n"},
		{EncodeStringArray(nil), "*0\r\n"},
		{EncodeStringArray([]string{"a", "bb"}), "*2\r\n$1\r\na\r\n$2\r\nbb\r\n"},
		{RespNil, "$-1\r\n"},
	}
	for _, tc := range cases {
		if !bytes.Equal(tc.got, []byte(tc.want)) {
			t.Errorf("got %q, want %q", tc.got, tc.want)
		}
	}
}

func TestEncodeNullableStringArray(t *testing.T) {
	a := "x"
	got := EncodeNullableStringArray([]*string{&a, nil})
	want := "*2\r\n$1\r\nx\r\n$-1\r\n"
	if string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestEncodeDecodeRoundTrip is the property the AOF depends on: anything we
// write must read back byte-identical, including spaces, newlines and NULs.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	inputs := [][]string{
		{"SET", "key", "value"},
		{"SET", "key with spaces", "value with spaces"},
		{"SET", "k", "multi\r\nline\r\nvalue"},
		{"SET", "k", "tab\there"},
		{"SET", "k", string([]byte{0x00, 0x01, 0xff})},
		{"SET", "k", ""},
		{"SET", "k", strings.Repeat("x", 100000)},
	}
	for _, in := range inputs {
		encoded := EncodeStringArray(in)
		v, n, err := DecodeOne(encoded)
		if err != nil {
			t.Fatalf("%v: decode failed: %v", in, err)
		}
		if n != len(encoded) {
			t.Errorf("%v: consumed %d of %d bytes", in, n, len(encoded))
		}
		got, err := DecodeArrayString(v.([]interface{}))
		if err != nil {
			t.Fatalf("%v: %v", in, err)
		}
		if len(got) != len(in) {
			t.Fatalf("%v: got %d tokens, want %d", in, len(got), len(in))
		}
		for i := range in {
			if got[i] != in[i] {
				t.Errorf("token %d: got %q, want %q", i, got[i], in[i])
			}
		}
	}
}

func itoa(n int) string {
	return formatInt64(int64(n))
}
