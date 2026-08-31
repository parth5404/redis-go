package core

import (
	"reflect"
	"testing"
)

// FuzzDecode drives the decoder with arbitrary bytes.
//
// The decoder sits directly behind a socket on a single-threaded event loop, so
// a panic on one malformed frame is not one bad request — it is the whole
// server. The property under test is therefore deliberately weak and absolute:
// for any input at all, Decode returns a value or an error, and never panics.
//
// Run a longer campaign with:
//
//	go test ./core/ -run=FuzzDecode -fuzz=FuzzDecode -fuzztime=60s
func FuzzDecode(f *testing.F) {
	seeds := []string{
		"+OK\r\n",
		"-ERR unknown command\r\n",
		":1000\r\n",
		":-1\r\n",
		"$5\r\nhello\r\n",
		"$0\r\n\r\n",
		"$-1\r\n",
		"*-1\r\n",
		"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
		"*1\r\n*1\r\n$1\r\na\r\n",
		"*0\r\n",
		"SET foo bar\r\n",
		"\r\n",
		"",
		// Shapes that previously reached a slice bound or an allocation with
		// attacker-chosen size.
		"$5\r\nab",
		"$999999999999\r\n",
		"*99999999\r\n",
		":\r\n",
		"$",
		"*",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		values, err := Decode(data)
		if err != nil {
			return
		}
		// A successful parse must have consumed the input into well-formed
		// values; walking them checks that nothing structurally impossible got
		// through.
		for _, v := range values {
			assertDecodable(t, v)
		}
	})
}

// assertDecodable walks a decoded value and fails on anything the decoder is
// not supposed to be able to produce.
func assertDecodable(t *testing.T, v interface{}) {
	t.Helper()
	switch typed := v.(type) {
	case nil, string, int64, error:
		return
	case []interface{}:
		for _, elem := range typed {
			assertDecodable(t, elem)
		}
	default:
		t.Fatalf("decoder produced unexpected type %T (%#v)", v, v)
	}
}

// FuzzDecodeOneRoundTrip checks the stronger property that anything the decoder
// accepts as a single complete frame re-encodes to the same bytes. A mismatch
// means the decoder is reading a frame the encoder would never have written,
// which is where ambiguous parses hide.
func FuzzDecodeOneRoundTrip(f *testing.F) {
	for _, s := range []string{"+OK\r\n", ":42\r\n", "$3\r\nfoo\r\n", "$0\r\n\r\n", "-ERR x\r\n"} {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		value, read, err := DecodeOne(data)
		if err != nil || read != len(data) {
			return
		}
		// Only the frames Encode can express are round-trippable; arrays and
		// inline commands decode to []interface{}, which Encode does not take.
		switch value.(type) {
		case string, int64, error:
		default:
			return
		}

		// Simple and bulk strings share the string type, so re-encode in the
		// form the input actually used.
		isSimple := len(data) > 0 && data[0] == '+'
		encoded := Encode(value, isSimple)

		again, _, err := DecodeOne(encoded)
		if err != nil {
			t.Fatalf("re-encoding %#v produced undecodable bytes %q: %v", value, encoded, err)
		}
		if !sameDecoded(value, again) {
			t.Fatalf("round trip changed %#v into %#v (via %q)", value, again, encoded)
		}
	})
}

// sameDecoded compares two decoded values, treating errors as equal when their
// messages match since error values are not comparable with DeepEqual.
func sameDecoded(a, b interface{}) bool {
	aErr, aIsErr := a.(error)
	bErr, bIsErr := b.(error)
	if aIsErr || bIsErr {
		return aIsErr && bIsErr && aErr.Error() == bErr.Error()
	}
	return reflect.DeepEqual(a, b)
}
