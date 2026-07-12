package core

import (
	"bytes"
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
	return readInline(data)
}

func readInline(data []byte) (interface{}, int, error) {
	pos := 0
	for i := 0; i < len(data); i++ {
		if data[i] == '\r' && i+1 < len(data) && data[i+1] == '\n' {
			pos = i
			break
		}
	}
	if pos == 0 && (len(data) == 0 || data[0] != '\r') {
		return nil, 0, fmt.Errorf("invalid protocol")
	}

	parts := bytes.Split(data[:pos], []byte(" "))
	var elems []interface{}
	for _, part := range parts {
		if len(part) > 0 {
			elems = append(elems, string(part))
		}
	}
	return elems, pos + 2, nil
}

func Decode(data []byte) ([]interface{}, error) {
	if len(data) == 0 {
		return nil, errors.New("no data")
	}
	var values []interface{} = make([]interface{}, 0)
	var index int = 0
	for index < len(data) {
		value, pos, err := DecodeOne(data[index:])
		if err != nil {
			return nil, err
		}
		values = append(values, value)
		index += pos
	}

	return values, nil
}

func DecodeArrayString(data []interface{}) ([]string, error) {
	tokens := make([]string, len(data))
	for k, v := range data {
		tokens[k] = v.(string)
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
