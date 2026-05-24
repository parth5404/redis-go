package core

import (
	"errors"
	"fmt"
)

func readLength(data []byte) (int, int) {
	pos, length := 0, 0
	for pos = range data {
		b := data[pos]
		if !(b >= '0' && b <= '9') {
			return length, pos + 2
		}
		length = length*10 + int(b-'0')
	}
	return 0, 0
}

func readSimpleString(data []byte) (string, int, error) {
	pos := 1
	for i := pos; i < len(data); i++ {
		pos = i
		if data[i] == '\r' {
			break
		}
	}
	return string(data[1:pos]), pos + 2, nil
}

func readError(data []byte) (string, int, error) {
	return readSimpleString(data)
}
func readInt64(data []byte) (int64, int, error) {
	pos := 0
	sign := int64(1)
	var value int64
	if data[1] >= '0' && data[1] <= '9' {
		pos = 1
	} else {
		pos = 2
		if data[1] == '-' {
			sign = -1
		}
	}
	for i := pos; i < len(data); i++ {
		if data[i] != '\r' {
			value = value*10 + int64(data[i]-'0')
			pos++
		} else {
			break
		}
	}
	return value * sign, pos + 2, nil
}
func readBulkString(data []byte) (string, int, error) {
	// first character $
	pos := 1

	len, delta := readLength(data[pos:])
	pos += delta

	// reading `len` bytes as string
	return string(data[pos:(pos + len)]), pos + len + 2, nil
}

func readArray(data []byte) (interface{}, int, error) {
	pos := 1
	count, delta := readLength(data[pos:])
	pos += delta
	var elems []interface{} = make([]interface{}, count)
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
		return nil, 0, errors.New("no data")
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
	return nil, 0, nil
}
func Decode(data []byte) (interface{}, error) {
	if len(data) == 0 {
		return nil, errors.New("no data")
	}
	value, _, err := DecodeOne(data)
	return value, err
}

func DecodeArrayString(data []byte) ([]string, error) {
	val, err := Decode(data)
	if err != nil {
		return nil, err
	}
	ts := val.([]interface{})
	tokens := make([]string, len(ts))
	for k, v := range ts {
		tokens[k] = v.(string)
	}
	return tokens, err
}

func Encode(value interface{}, isSimple bool) []byte {
	switch v := value.(type) {
	case string:
		if isSimple {
			return []byte(fmt.Sprintf("+%s\r\n", v))
		}
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
	}
	return nil
}
