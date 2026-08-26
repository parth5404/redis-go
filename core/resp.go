package core

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

// RESP (REdis Serialization Protocol) decoding.
//
// # The contract that makes this safe
//
// Every reader takes the *whole* remaining buffer and returns how many bytes it
// consumed. It must never index past len(data), and must distinguish two kinds
// of failure:
//
//   - ErrIncomplete: the bytes so far are a valid prefix of a frame. The caller
//     should read more from the socket and retry. NOT an error to report.
//   - anything else: the bytes are malformed. The connection is unrecoverable
//     (we cannot know where the next frame starts) and must be closed.
//
// Conflating those two is the root of most naive parser bugs. The original code
// had no notion of either: it assumed one read() always delivered exactly one
// complete command, and sliced blindly. Because a Go slice can be re-sliced up
// to its *capacity*, `data[pos : pos+declaredLen]` on a short read either
// silently returned unrelated bytes from the rest of the buffer, or -- once the
// declared length exceeded the buffer capacity -- panicked and killed the whole
// process. A 13-byte payload was enough:
//
//	*1\r\n$5000\r\nab   ->  panic: slice bounds out of range [:5007] with capacity 508
//
// Every bound below exists because of that.

// ErrIncomplete signals a valid but partial frame: read more and retry.
var ErrIncomplete = errors.New("incomplete RESP frame")

// Protocol limits, matching Redis's own caps. Without these, a client can name
// an arbitrary length and force an arbitrary allocation.
const (
	// maxBulkLength is Redis's proto-max-bulk-len (512 MB).
	maxBulkLength = 512 * 1024 * 1024
	// maxMultiBulkCount is Redis's 1024*1024 element cap on arrays. Without
	// it, `*999999999\r\n` makes us allocate a billion interface headers
	// (16 GB) and the process is OOM-killed by a 12-byte request.
	maxMultiBulkCount = 1024 * 1024
	// maxInlineSize is Redis's PROTO_INLINE_MAX_SIZE (64 KB).
	maxInlineSize = 64 * 1024
	// maxNestingDepth stops `*1\r\n*1\r\n*1\r\n...` from recursing until the
	// goroutine stack is exhausted.
	maxNestingDepth = 32
)

var (
	errBadLength    = errors.New("ERR Protocol error: invalid multibulk length")
	errBadBulk      = errors.New("ERR Protocol error: invalid bulk length")
	errBadInt       = errors.New("ERR Protocol error: invalid integer")
	errTooBig       = errors.New("ERR Protocol error: too big inline request")
	errBadType      = errors.New("ERR Protocol error: unknown reply type")
	errTooDeep      = errors.New("ERR Protocol error: nesting too deep")
	errUnbalanced   = errors.New("ERR Protocol error: unbalanced quotes in request")
	errExpectedCRLF = errors.New("ERR Protocol error: expected CRLF")
)

// indexCRLF returns the offset of the first CRLF in data, or -1.
func indexCRLF(data []byte) int {
	for i := 0; i+1 < len(data); i++ {
		if data[i] == '\r' && data[i+1] == '\n' {
			return i
		}
	}
	return -1
}

// readSignedLength parses the `<number>\r\n` header shared by $ and * frames.
// data must start at the number, i.e. the type byte is already consumed.
// Returns the value and the bytes consumed including the CRLF.
func readSignedLength(data []byte) (int, int, error) {
	end := indexCRLF(data)
	if end < 0 {
		// No CRLF yet. Could still be coming -- unless the client has
		// already sent more digits than any legal length could need, in
		// which case it is malformed and we must not buffer forever.
		if len(data) > 32 {
			return 0, 0, errBadLength
		}
		return 0, 0, ErrIncomplete
	}
	if end == 0 {
		return 0, 0, errBadLength
	}
	n, err := strconv.Atoi(string(data[:end]))
	if err != nil {
		return 0, 0, errBadLength
	}
	return n, end + 2, nil
}

// readSimpleString decodes `+<text>\r\n`.
func readSimpleString(data []byte) (string, int, error) {
	end := indexCRLF(data[1:])
	if end < 0 {
		if len(data) > maxInlineSize {
			return "", 0, errTooBig
		}
		return "", 0, ErrIncomplete
	}
	return string(data[1 : 1+end]), 1 + end + 2, nil
}

// readError decodes `-<message>\r\n`.
func readError(data []byte) (string, int, error) { return readSimpleString(data) }

// readInt64 decodes `:<number>\r\n`.
//
// The original hand-rolled digit loop indexed data[1] before checking the
// length (so a bare ":" panicked) and accumulated any byte as a digit, so
// ":abc" silently produced garbage instead of an error.
func readInt64(data []byte) (int64, int, error) {
	end := indexCRLF(data[1:])
	if end < 0 {
		if len(data) > 32 {
			return 0, 0, errBadInt
		}
		return 0, 0, ErrIncomplete
	}
	v, err := strconv.ParseInt(string(data[1:1+end]), 10, 64)
	if err != nil {
		return 0, 0, errBadInt
	}
	return v, 1 + end + 2, nil
}

// readBulkString decodes `$<len>\r\n<len bytes>\r\n`.
//
// `$-1\r\n` is the RESP null bulk string and decodes to a nil interface, which
// is how a caller distinguishes "absent" from "empty string".
func readBulkString(data []byte) (interface{}, int, error) {
	n, hdr, err := readSignedLength(data[1:])
	if err != nil {
		return nil, 0, err
	}
	pos := 1 + hdr

	if n == -1 {
		return nil, pos, nil
	}
	if n < 0 || n > maxBulkLength {
		return nil, 0, errBadBulk
	}
	// The +2 is the trailing CRLF. Checking for it before slicing is what
	// makes the slice below provably in range.
	if len(data) < pos+n+2 {
		return nil, 0, ErrIncomplete
	}
	if data[pos+n] != '\r' || data[pos+n+1] != '\n' {
		return nil, 0, errExpectedCRLF
	}
	return string(data[pos : pos+n]), pos + n + 2, nil
}

// readArray decodes `*<count>\r\n<count frames>`.
func readArray(data []byte, depth int) (interface{}, int, error) {
	if depth > maxNestingDepth {
		return nil, 0, errTooDeep
	}
	n, hdr, err := readSignedLength(data[1:])
	if err != nil {
		return nil, 0, err
	}
	pos := 1 + hdr

	if n == -1 {
		return nil, pos, nil
	}
	if n < 0 || n > maxMultiBulkCount {
		return nil, 0, errBadLength
	}

	// Allocate lazily in append rather than make([]interface{}, n): a
	// declared count of 1,048,576 with no payload should cost nothing until
	// the elements actually arrive.
	elems := make([]interface{}, 0, minInt(n, 64))
	for i := 0; i < n; i++ {
		if pos >= len(data) {
			return nil, 0, ErrIncomplete
		}
		elem, used, err := decodeOne(data[pos:], depth+1)
		if err != nil {
			return nil, 0, err
		}
		elems = append(elems, elem)
		pos += used
	}
	return elems, pos, nil
}

// readInline decodes a bare command line like `PING\r\n`, the "inline command"
// form real Redis accepts so you can drive it from telnet or netcat.
//
// Supports single- and double-quoted arguments so values containing spaces can
// be sent inline, matching Redis's sdssplitargs.
func readInline(data []byte) (interface{}, int, error) {
	end := indexCRLF(data)
	if end < 0 {
		if len(data) > maxInlineSize {
			return nil, 0, errTooBig
		}
		return nil, 0, ErrIncomplete
	}
	line := data[:end]
	args, err := splitInlineArgs(line)
	if err != nil {
		return nil, 0, err
	}
	elems := make([]interface{}, len(args))
	for i, a := range args {
		elems[i] = a
	}
	return elems, end + 2, nil
}

// splitInlineArgs tokenises an inline command line, honouring quotes.
func splitInlineArgs(line []byte) ([]string, error) {
	var out []string
	i := 0
	for i < len(line) {
		for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
			i++
		}
		if i >= len(line) {
			break
		}
		var tok []byte
		switch line[i] {
		case '"', '\'':
			quote := line[i]
			i++
			closed := false
			for i < len(line) {
				if line[i] == '\\' && quote == '"' && i+1 < len(line) {
					i++
					tok = append(tok, unescapeInline(line[i]))
					i++
					continue
				}
				if line[i] == quote {
					i++
					closed = true
					break
				}
				tok = append(tok, line[i])
				i++
			}
			if !closed {
				return nil, errUnbalanced
			}
		default:
			for i < len(line) && line[i] != ' ' && line[i] != '\t' {
				tok = append(tok, line[i])
				i++
			}
		}
		out = append(out, string(tok))
	}
	return out, nil
}

func unescapeInline(b byte) byte {
	switch b {
	case 'n':
		return '\n'
	case 'r':
		return '\r'
	case 't':
		return '\t'
	default:
		return b
	}
}

// DecodeOne decodes the first RESP value in data, returning the value and the
// number of bytes consumed.
//
// Callers must check errors.Is(err, ErrIncomplete) and treat it as "read more",
// not as a failure.
func DecodeOne(data []byte) (interface{}, int, error) { return decodeOne(data, 0) }

func decodeOne(data []byte, depth int) (interface{}, int, error) {
	if len(data) == 0 {
		return nil, 0, ErrIncomplete
	}
	switch data[0] {
	case '+':
		s, n, err := readSimpleString(data)
		return s, n, err
	case '-':
		s, n, err := readError(data)
		return s, n, err
	case ':':
		v, n, err := readInt64(data)
		return v, n, err
	case '$':
		return readBulkString(data)
	case '*':
		return readArray(data, depth)
	case '\r', '\n':
		// A stray newline between frames. Redis tolerates it; skipping one
		// byte keeps us in sync instead of erroring out.
		return nil, 1, nil
	default:
		if data[0] < 0x20 || data[0] == 0x7f {
			// Control bytes are never a legal inline command and are a
			// reliable sign the peer is speaking a different protocol.
			return nil, 0, errBadType
		}
		return readInline(data)
	}
}

// Decode decodes every complete RESP value in data.
//
// Used for whole-buffer inputs such as replaying an AOF file. Network callers
// should use DecodeOne in a loop so they can act on ErrIncomplete; see
// server.Client.
func Decode(data []byte) ([]interface{}, error) {
	if len(data) == 0 {
		return nil, errors.New("no data")
	}
	values := make([]interface{}, 0, 8)
	index := 0
	for index < len(data) {
		value, used, err := decodeOne(data[index:], 0)
		if err != nil {
			if errors.Is(err, ErrIncomplete) {
				// Trailing partial frame: stop and return what we have.
				// A truncated AOF (killed mid-write) is normal and Redis
				// tolerates it the same way.
				break
			}
			return nil, err
		}
		// Guard against a reader that consumes nothing. Without this, any
		// such bug becomes an infinite loop that pins a core instead of a
		// test failure.
		if used <= 0 {
			return nil, fmt.Errorf("decoder made no progress at offset %d", index)
		}
		values = append(values, value)
		index += used
	}
	return values, nil
}

// DecodeArrayString converts a decoded RESP array into a string slice.
//
// Every element is converted rather than type-asserted: the original code did
// `v.(string)` unchecked, so a client sending an integer argument
// (`*2\r\n$3\r\nGET\r\n:5\r\n`) panicked the server.
func DecodeArrayString(data []interface{}) ([]string, error) {
	tokens := make([]string, len(data))
	for i, v := range data {
		switch t := v.(type) {
		case string:
			tokens[i] = t
		case int64:
			tokens[i] = formatInt64(t)
		case nil:
			tokens[i] = ""
		default:
			return nil, fmt.Errorf("ERR Protocol error: expected a bulk string, got %T", v)
		}
	}
	return tokens, nil
}

// --- Encoding ---------------------------------------------------------------

// RespNil is the RESP null bulk string.
var RespNil = []byte("$-1\r\n")

// RespEmptyArray is the RESP empty array.
var RespEmptyArray = []byte("*0\r\n")

// RespOK is the canonical +OK reply.
var RespOK = []byte("+OK\r\n")

// EncodeSimpleString builds `+<s>\r\n`.
func EncodeSimpleString(s string) []byte { return []byte("+" + s + "\r\n") }

// EncodeError builds `-<msg>\r\n`.
func EncodeError(err error) []byte { return []byte("-" + err.Error() + "\r\n") }

// EncodeErrorf builds an error reply from a format string.
func EncodeErrorf(format string, a ...interface{}) []byte {
	return []byte("-" + fmt.Sprintf(format, a...) + "\r\n")
}

// EncodeInt builds `:<n>\r\n`.
func EncodeInt(n int64) []byte { return []byte(":" + formatInt64(n) + "\r\n") }

// EncodeBulkString builds `$<len>\r\n<s>\r\n`.
func EncodeBulkString(s string) []byte {
	var b bytes.Buffer
	b.Grow(len(s) + 16)
	b.WriteByte('$')
	b.WriteString(formatInt64(int64(len(s))))
	b.WriteString("\r\n")
	b.WriteString(s)
	b.WriteString("\r\n")
	return b.Bytes()
}

// EncodeStringArray builds an array of bulk strings.
func EncodeStringArray(vals []string) []byte {
	var b bytes.Buffer
	b.WriteByte('*')
	b.WriteString(formatInt64(int64(len(vals))))
	b.WriteString("\r\n")
	for _, v := range vals {
		b.Write(EncodeBulkString(v))
	}
	return b.Bytes()
}

// EncodeNullableStringArray builds an array where a nil entry becomes a RESP
// null bulk string, which is how MGET reports missing keys.
func EncodeNullableStringArray(vals []*string) []byte {
	var b bytes.Buffer
	b.WriteByte('*')
	b.WriteString(formatInt64(int64(len(vals))))
	b.WriteString("\r\n")
	for _, v := range vals {
		if v == nil {
			b.Write(RespNil)
			continue
		}
		b.Write(EncodeBulkString(*v))
	}
	return b.Bytes()
}

// Encode serialises a Go value to RESP. isSimple selects the simple-string
// form over the bulk-string form for strings.
//
// Kept for compatibility with existing callers (the AOF writer and the MCP
// bridge). New code should prefer the explicit Encode* helpers above, which
// cannot silently fall through to a nil reply on an unexpected type.
func Encode(value interface{}, isSimple bool) []byte {
	switch v := value.(type) {
	case nil:
		return RespNil
	case string:
		if isSimple {
			return EncodeSimpleString(v)
		}
		return EncodeBulkString(v)
	case []byte:
		return EncodeBulkString(string(v))
	case int:
		return EncodeInt(int64(v))
	case int8:
		return EncodeInt(int64(v))
	case int16:
		return EncodeInt(int64(v))
	case int32:
		return EncodeInt(int64(v))
	case int64:
		return EncodeInt(v)
	case bool:
		if v {
			return EncodeInt(1)
		}
		return EncodeInt(0)
	case []string:
		return EncodeStringArray(v)
	case []interface{}:
		var b bytes.Buffer
		b.WriteByte('*')
		b.WriteString(formatInt64(int64(len(v))))
		b.WriteString("\r\n")
		for _, e := range v {
			b.Write(Encode(e, false))
		}
		return b.Bytes()
	case error:
		return EncodeError(v)
	}
	return RespNil
}
