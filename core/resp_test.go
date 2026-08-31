package core

import (
	"errors"
	"reflect"
	"testing"
)

func TestDecodeOne(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		want     interface{}
		wantRead int
	}{
		{name: "simple string", input: "+OK\r\n", want: "OK", wantRead: 5},
		{name: "empty simple string", input: "+\r\n", want: "", wantRead: 3},
		{name: "integer", input: ":1000\r\n", want: int64(1000), wantRead: 7},
		{name: "negative integer", input: ":-42\r\n", want: int64(-42), wantRead: 6},
		{name: "explicitly positive integer", input: ":+7\r\n", want: int64(7), wantRead: 5},
		{name: "zero", input: ":0\r\n", want: int64(0), wantRead: 4},
		{name: "bulk string", input: "$5\r\nhello\r\n", want: "hello", wantRead: 11},
		{name: "empty bulk string", input: "$0\r\n\r\n", want: "", wantRead: 6},
		{name: "bulk string with spaces", input: "$7\r\nfoo bar\r\n", want: "foo bar", wantRead: 13},
		{name: "bulk string with crlf inside", input: "$4\r\na\r\nb\r\n", want: "a\r\nb", wantRead: 10},
		{name: "null bulk string", input: "$-1\r\n", want: nil, wantRead: 5},
		{name: "null array", input: "*-1\r\n", want: nil, wantRead: 5},
		{
			name:     "array of bulk strings",
			input:    "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
			want:     []interface{}{"foo", "bar"},
			wantRead: 22,
		},
		{
			name:     "empty array",
			input:    "*0\r\n",
			want:     []interface{}{},
			wantRead: 4,
		},
		{
			name:     "nested array",
			input:    "*2\r\n*1\r\n$1\r\na\r\n:5\r\n",
			want:     []interface{}{[]interface{}{"a"}, int64(5)},
			wantRead: 19,
		},
		{
			name:     "inline command",
			input:    "SET foo bar\r\n",
			want:     []interface{}{"SET", "foo", "bar"},
			wantRead: 13,
		},
		{
			name:     "inline command with repeated spaces",
			input:    "GET   foo\r\n",
			want:     []interface{}{"GET", "foo"},
			wantRead: 11,
		},
		{name: "blank inline line", input: "\r\n", want: nil, wantRead: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, read, err := DecodeOne([]byte(tt.input))
			if err != nil {
				t.Fatalf("DecodeOne(%q) returned error: %v", tt.input, err)
			}
			if read != tt.wantRead {
				t.Errorf("DecodeOne(%q) read %d bytes, want %d", tt.input, read, tt.wantRead)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DecodeOne(%q) = %#v, want %#v", tt.input, got, tt.want)
			}
		})
	}
}

// TestDecodeOneError decodes an error frame into a Go error value rather than a
// bare string. The MCP bridge relies on this to tell a rejected command apart
// from a successful one.
func TestDecodeOneError(t *testing.T) {
	got, read, err := DecodeOne([]byte("-ERR unknown command 'FOO'\r\n"))
	if err != nil {
		t.Fatalf("DecodeOne returned error: %v", err)
	}
	if read != 28 {
		t.Errorf("read %d bytes, want 28", read)
	}

	respErr, ok := got.(error)
	if !ok {
		t.Fatalf("decoded %#v (%T), want an error value", got, got)
	}
	if respErr.Error() != "ERR unknown command 'FOO'" {
		t.Errorf("error message = %q", respErr.Error())
	}
}

// TestDecodeOneMalformed covers frames a hostile or buggy client can put on the
// wire. Every one of these used to slice, index or allocate its way into a
// panic, which on a single-threaded event loop takes down every connection at
// once, so the requirement is an error return and never a crash.
func TestDecodeOneMalformed(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "empty input", input: ""},
		{name: "bulk string shorter than declared", input: "$5\r\nab"},
		{name: "bulk string missing payload", input: "$5\r\n"},
		{name: "bulk string missing terminator", input: "$2\r\nab"},
		{name: "bulk length not a number", input: "$abc\r\n"},
		{name: "bulk length with no digits", input: "$\r\n"},
		{name: "bulk length absurdly large", input: "$999999999999999\r\nx\r\n"},
		{name: "array claims more elements than bytes", input: "*99999999\r\n"},
		{name: "array truncated mid element", input: "*2\r\n$3\r\nfoo\r\n"},
		{name: "array length not a number", input: "*x\r\n"},
		{name: "integer with no digits", input: ":\r\n"},
		{name: "integer with trailing garbage", input: ":12x\r\n"},
		{name: "integer truncated", input: ":12"},
		{name: "lone type byte", input: "$"},
		{name: "simple string unterminated", input: "+OK"},
		{name: "error unterminated", input: "-ERR"},
		{name: "cr without lf", input: "+OK\rx"},
		{name: "inline without terminator", input: "SET foo bar"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := DecodeOne([]byte(tt.input))
			if err == nil {
				t.Errorf("DecodeOne(%q) returned no error, want one", tt.input)
			}
		})
	}
}

func TestDecodePipeline(t *testing.T) {
	// Three commands arriving in a single read, the way a pipelining client
	// sends them.
	input := "*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n"

	values, err := Decode([]byte(input))
	if err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}
	want := []interface{}{
		[]interface{}{"PING"},
		[]interface{}{"SET", "a", "1"},
		[]interface{}{"GET", "a"},
	}
	if !reflect.DeepEqual(values, want) {
		t.Errorf("Decode = %#v, want %#v", values, want)
	}
}

func TestDecodeRejectsTruncatedTail(t *testing.T) {
	// A complete frame followed by a partial one must not be reported as a
	// clean parse.
	if _, err := Decode([]byte("+OK\r\n$5\r\nab")); err == nil {
		t.Error("Decode accepted a truncated trailing frame")
	}
}

func TestEncode(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		isSimple bool
		want     string
	}{
		{name: "simple string", value: "OK", isSimple: true, want: "+OK\r\n"},
		{name: "bulk string", value: "hello", want: "$5\r\nhello\r\n"},
		{name: "empty bulk string", value: "", want: "$0\r\n\r\n"},
		{name: "int64", value: int64(42), want: ":42\r\n"},
		{name: "negative int64", value: int64(-2), want: ":-2\r\n"},
		{name: "error", value: errors.New("ERR nope"), want: "-ERR nope\r\n"},
		{name: "string array", value: []string{"a", "b"}, want: "*2\r\n$1\r\na\r\n$1\r\nb\r\n"},
		{name: "empty string array", value: []string{}, want: "*0\r\n"},
		{name: "unsupported type falls back to nil", value: 3.14, want: "$-1\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(Encode(tt.value, tt.isSimple))
			if got != tt.want {
				t.Errorf("Encode(%#v, %v) = %q, want %q", tt.value, tt.isSimple, got, tt.want)
			}
		})
	}
}

// TestEncodeDecodeRoundTrip pins down that the two halves of the protocol agree
// with each other.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{name: "bulk string", value: "hello world"},
		{name: "empty string", value: ""},
		{name: "string holding crlf", value: "a\r\nb"},
		{name: "int64", value: int64(1234567890)},
		{name: "negative int64", value: int64(-98765)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := Encode(tt.value, false)
			got, read, err := DecodeOne(encoded)
			if err != nil {
				t.Fatalf("DecodeOne(%q) returned error: %v", encoded, err)
			}
			if read != len(encoded) {
				t.Errorf("read %d bytes of %d", read, len(encoded))
			}
			if !reflect.DeepEqual(got, tt.value) {
				t.Errorf("round trip gave %#v, want %#v", got, tt.value)
			}
		})
	}
}

func TestDecodeArrayString(t *testing.T) {
	got, err := DecodeArrayString([]interface{}{"SET", "k", "v"})
	if err != nil {
		t.Fatalf("DecodeArrayString returned error: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"SET", "k", "v"}) {
		t.Errorf("DecodeArrayString = %#v", got)
	}

	// A nested array where a bulk string was expected is a protocol violation,
	// not something to type-assert straight through.
	if _, err := DecodeArrayString([]interface{}{"SET", []interface{}{"k"}}); err == nil {
		t.Error("DecodeArrayString accepted a non-string element")
	}
}
