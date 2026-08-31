package core

import "errors"

func getType(te uint8) uint8 {
	return (te >> 4) << 4
}

func getEncoding(te uint8) uint8 {
	return te & 0b00001111
}

// The two assertions below produce messages that go straight onto the wire, so
// they use the wording real Redis clients already know how to interpret.

func assertType(te uint8, t uint8) error {
	if getType(te) != t {
		return errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return nil
}

func assertEncoding(te uint8, e uint8) error {
	if getEncoding(te) != e {
		return errors.New("ERR value is not an integer or out of range")
	}
	return nil
}

// typeName renders the high nibble the way Redis reports it for TYPE.
func typeName(te uint8) string {
	switch getType(te) {
	case OBJ_TYPE_STRING:
		return "string"
	}
	return "none"
}

// encodingName renders the low nibble the way Redis reports it for
// OBJECT ENCODING.
func encodingName(te uint8) string {
	switch getEncoding(te) {
	case OBJ_ENCODING_INT:
		return "int"
	case oBJ_ENCODING_EMBSTR:
		return "embstr"
	}
	return "raw"
}
