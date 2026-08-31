package core

import (
	"bytes"
	"errors"
	"fmt"
)

// Every frame the decoder sees arrives from a socket, so a length prefix is
// attacker-controlled input. maxLengthPrefix caps how large a bulk string or
// array header may claim to be; it matches Redis' own proto-max-bulk-len and
// also keeps length*10 arithmetic from overflowing an int.
const maxLengthPrefix = 512 * 1024 * 1024

var (
	// ErrIncompleteRESP means the bytes seen so far are a valid prefix of a
	// frame but the frame has not fully arrived yet.
	ErrIncompleteRESP = errors.New("incomplete RESP frame")
	// ErrInvalidRESP means the bytes cannot begin a valid frame at all.
	ErrInvalidRESP = errors.New("invalid RESP frame")
)

// readLength parses the decimal digits of a length prefix and returns the value
// plus the number of bytes consumed, trailing CRLF included.
func readLength(data []byte) (int, int, error) {
	length := 0
	for pos := 0; pos < len(data); pos++ {
		b := data[pos]
		if b == '\r' {
			if pos == 0 {
				return 0, 0, ErrInvalidRESP
			}
			if pos+1 >= len(data) {
				return 0, 0, ErrIncompleteRESP
			}
			if data[pos+1] != '\n' {
				return 0, 0, ErrInvalidRESP
			}
			return length, pos + 2, nil
		}
		if b < '0' || b > '9' {
			return 0, 0, ErrInvalidRESP
		}
		length = length*10 + int(b-'0')
		if length > maxLengthPrefix {
			return 0, 0, ErrInvalidRESP
		}
	}
	return 0, 0, ErrIncompleteRESP
}

// readSignedLength parses a length prefix that is allowed to be negative, since
// RESP spells the null bulk string as $-1 and the null array as *-1.
func readSignedLength(data []byte) (int, int, error) {
	if len(data) > 0 && data[0] == '-' {
		length, delta, err := readLength(data[1:])
		if err != nil {
			return 0, 0, err
		}
		return -length, delta + 1, nil
	}
	return readLength(data)
}

func readSimpleString(data []byte) (string, int, error) {
	for i := 1; i < len(data); i++ {
		if data[i] != '\r' {
			continue
		}
		if i+1 >= len(data) {
			return "", 0, ErrIncompleteRESP
		}
		if data[i+1] != '\n' {
			return "", 0, ErrInvalidRESP
		}
		return string(data[1:i]), i + 2, nil
	}
	return "", 0, ErrIncompleteRESP
}

// readError decodes a "-ERR ..." frame into a Go error value rather than a
// plain string. Callers that only see the decoded value — the MCP bridge, for
// one — can then tell a failed command apart from a successful "+OK" without
// re-inspecting the wire bytes.
func readError(data []byte) (interface{}, int, error) {
	msg, delta, err := readSimpleString(data)
	if err != nil {
		return nil, 0, err
	}
	return errors.New(msg), delta, nil
}

func readInt64(data []byte) (int64, int, error) {
	if len(data) < 2 {
		return 0, 0, ErrIncompleteRESP
	}
	pos := 1
	sign := int64(1)
	if data[pos] == '-' || data[pos] == '+' {
		if data[pos] == '-' {
			sign = -1
		}
		pos++
	}

	start := pos
	var value int64
	for ; pos < len(data); pos++ {
		b := data[pos]
		if b == '\r' {
			if pos == start {
				return 0, 0, ErrInvalidRESP
			}
			if pos+1 >= len(data) {
				return 0, 0, ErrIncompleteRESP
			}
			if data[pos+1] != '\n' {
				return 0, 0, ErrInvalidRESP
			}
			return value * sign, pos + 2, nil
		}
		if b < '0' || b > '9' {
			return 0, 0, ErrInvalidRESP
		}
		value = value*10 + int64(b-'0')
		if value < 0 {
			return 0, 0, ErrInvalidRESP
		}
	}
	return 0, 0, ErrIncompleteRESP
}

func readBulkString(data []byte) (interface{}, int, error) {
	// first character is $
	pos := 1
	length, delta, err := readSignedLength(data[pos:])
	if err != nil {
		return nil, 0, err
	}
	pos += delta

	if length < 0 {
		return nil, pos, nil
	}
	// The declared payload and its trailing CRLF must both have arrived before
	// the slice below is safe to take.
	if pos+length+2 > len(data) {
		return nil, 0, ErrIncompleteRESP
	}
	return string(data[pos : pos+length]), pos + length + 2, nil
}

func readArray(data []byte) (interface{}, int, error) {
	pos := 1
	count, delta, err := readSignedLength(data[pos:])
	if err != nil {
		return nil, 0, err
	}
	pos += delta

	if count < 0 {
		return nil, pos, nil
	}
	// A client may claim any element count it likes. Every element needs at
	// least one byte on the wire, so refuse to preallocate for a count the
	// remaining buffer could not possibly hold.
	if count > len(data)-pos {
		return nil, 0, ErrIncompleteRESP
	}

	elems := make([]interface{}, count)
	for i := range elems {
		elem, delta, err := DecodeOne(data[pos:])
		if err != nil {
			return nil, 0, err
		}
		elems[i] = elem
		pos += delta
	}
	return elems, pos, nil
}

func DecodeOne(data []byte) (interface{}, int, error) {
	if len(data) == 0 {
		return nil, 0, ErrIncompleteRESP
	}
	switch data[0] {
	case '+':
		return readSimpleString(data)
	case '-':
		return readError(data)
	case ':':
		return readInt64(data)
	case '$':
		return readBulkString(data)
	case '*':
		return readArray(data)
	}
	return readInline(data)
}

// readInline handles the plain-text form redis-cli falls back to and that a
// bare telnet session produces, e.g. "SET foo bar\r\n".
func readInline(data []byte) (interface{}, int, error) {
	end := bytes.Index(data, []byte("\r\n"))
	if end < 0 {
		return nil, 0, ErrIncompleteRESP
	}

	var elems []interface{}
	for _, part := range bytes.Split(data[:end], []byte(" ")) {
		if len(part) > 0 {
			elems = append(elems, string(part))
		}
	}
	// A blank line is a no-op rather than a command; returning nil lets the
	// caller skip it while still consuming the bytes.
	if len(elems) == 0 {
		return nil, end + 2, nil
	}
	return elems, end + 2, nil
}

func Decode(data []byte) ([]interface{}, error) {
	if len(data) == 0 {
		return nil, ErrIncompleteRESP
	}

	values := make([]interface{}, 0)
	for index := 0; index < len(data); {
		value, delta, err := DecodeOne(data[index:])
		if err != nil {
			return nil, err
		}
		// A decoder that reported success without consuming anything would
		// spin this loop forever.
		if delta <= 0 {
			return nil, ErrInvalidRESP
		}
		values = append(values, value)
		index += delta
	}
	return values, nil
}

func DecodeArrayString(data []interface{}) ([]string, error) {
	tokens := make([]string, len(data))
	for k, v := range data {
		s, ok := v.(string)
		if !ok {
			return nil, ErrInvalidRESP
		}
		tokens[k] = s
	}
	return tokens, nil
}

func Encode(value interface{}, isSimple bool) []byte {
	switch v := value.(type) {
	case string:
		if isSimple {
			return []byte(fmt.Sprintf("+%s\r\n", v))
		}
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
	case int, int8, int16, int32, int64:
		return []byte(fmt.Sprintf(":%d\r\n", v))
	case []string:
		var b []byte
		buf := bytes.NewBuffer(b)
		for _, val := range v {
			buf.Write(Encode(val, false))
		}
		return []byte(fmt.Sprintf("*%d\r\n%s", len(v), buf.Bytes()))
	case error:
		return []byte(fmt.Sprintf("-%s\r\n", v.Error()))
	}
	return RESP_NIL
}
