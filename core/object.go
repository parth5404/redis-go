package core

// Obj is a stored value. TypeEncoding packs two fields into a single byte: the
// high nibble holds the type and the low nibble the encoding, so an object
// carries one byte of metadata instead of two separate fields.
//
//	 7 6 5 4   3 2 1 0
//	+-------+ +-------+
//	| type  | | enc   |
//
// ExpiresAt is a Unix millisecond timestamp, or -1 for a key that never expires.
type Obj struct {
	TypeEncoding uint8
	Value        interface{}
	ExpiresAt    int64
}

// Types occupy the high nibble.
var OBJ_TYPE_STRING uint8 = 0 << 4

// Encodings occupy the low nibble. The thresholds mirror Redis: values that
// parse as integers are stored as int, short strings as embstr, longer ones raw.
var OBJ_ENCODING_RAW uint8 = 0
var OBJ_ENCODING_INT uint8 = 1
var oBJ_ENCODING_EMBSTR uint8 = 8
