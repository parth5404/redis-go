package core

import (
	"strings"
	"time"
)

func evalDEL(args []string) []byte {
	n := 0
	for _, k := range args {
		if KV.Del(k) {
			n++
		}
	}
	return EncodeInt(int64(n))
}

func evalEXISTS(args []string) []byte {
	// Redis counts each key separately, so EXISTS k k returns 2 if k exists.
	n := 0
	for _, k := range args {
		if KV.Exists(k) {
			n++
		}
	}
	return EncodeInt(int64(n))
}

// expireCommon implements EXPIRE and PEXPIRE.
//
// unitMs is the multiplier that converts the supplied number into
// milliseconds. Both commands support the NX/XX/GT/LT flags Redis 7 added.
func expireCommon(args []string, unitMs int64) []byte {
	key := args[0]
	n, err := parseInt64(args[1])
	if err != nil {
		return EncodeError(ErrNotInteger)
	}

	var nx, xx, gt, lt bool
	for _, f := range args[2:] {
		switch strings.ToUpper(f) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GT":
			gt = true
		case "LT":
			lt = true
		default:
			return EncodeError(ErrSyntax)
		}
	}
	// NX is mutually exclusive with the others, and GT with LT.
	if (nx && (xx || gt || lt)) || (gt && lt) {
		return EncodeErrorf("ERR NX and XX, GT or LT options at the same time are not compatible")
	}

	obj := KV.Get(key)
	if obj == nil {
		return EncodeInt(0)
	}

	// A non-positive TTL deletes the key immediately, which is what Redis
	// does rather than storing a timestamp in the past.
	if n <= 0 {
		KV.Del(key)
		return EncodeInt(1)
	}

	now := time.Now().UnixMilli()
	if n > (maxInt64-now)/unitMs {
		return EncodeError(ErrInvalidExpire)
	}
	newExpiry := now + n*unitMs

	current := obj.ExpiresAt
	switch {
	case nx && current != NoExpiry:
		return EncodeInt(0)
	case xx && current == NoExpiry:
		return EncodeInt(0)
	case gt && (current == NoExpiry || newExpiry <= current):
		// GT against a key with no expiry never applies: "no expiry" is
		// conceptually infinite, so nothing is greater than it.
		return EncodeInt(0)
	case lt && current != NoExpiry && newExpiry >= current:
		return EncodeInt(0)
	}

	if !KV.SetExpiry(key, newExpiry) {
		return EncodeInt(0)
	}
	return EncodeInt(1)
}

func evalEXPIRE(args []string) []byte  { return expireCommon(args, 1000) }
func evalPEXPIRE(args []string) []byte { return expireCommon(args, 1) }

// ttlCommon implements TTL and PTTL.
//
// Return values follow Redis: -2 means the key does not exist, -1 means it
// exists with no expiry.
func ttlCommon(args []string, divisor int64) []byte {
	obj := KV.Get(args[0])
	if obj == nil {
		return EncodeInt(-2)
	}
	if obj.ExpiresAt == NoExpiry {
		return EncodeInt(-1)
	}
	left := obj.ExpiresAt - time.Now().UnixMilli()
	if left < 0 {
		return EncodeInt(-2)
	}
	if divisor == 1000 {
		// Round to nearest second rather than truncating: Redis reports 10
		// for a key set with EX 10, but truncation gives 9 as soon as a
		// single millisecond has elapsed.
		return EncodeInt((left + 999) / 1000)
	}
	return EncodeInt(left)
}

func evalTTL(args []string) []byte  { return ttlCommon(args, 1000) }
func evalPTTL(args []string) []byte { return ttlCommon(args, 1) }

func evalPERSIST(args []string) []byte {
	key := args[0]
	obj := KV.Get(key)
	if obj == nil || obj.ExpiresAt == NoExpiry {
		return EncodeInt(0)
	}
	if !KV.SetExpiry(key, NoExpiry) {
		return EncodeInt(0)
	}
	return EncodeInt(1)
}

func evalTYPE(args []string) []byte {
	obj := KV.Get(args[0])
	if obj == nil {
		return EncodeSimpleString("none")
	}
	return EncodeSimpleString(obj.TypeName())
}

func evalKEYS(args []string) []byte {
	return EncodeStringArray(KV.Keys(args[0]))
}

func evalRENAME(args []string) []byte {
	if !KV.Rename(args[0], args[1]) {
		return EncodeError(ErrNoSuchKey)
	}
	return RespOK
}

func evalOBJECT(args []string) []byte {
	switch strings.ToUpper(args[0]) {
	case "ENCODING":
		if len(args) != 2 {
			return EncodeError(wrongArity("object|encoding"))
		}
		obj := KV.Get(args[1])
		if obj == nil {
			return EncodeError(ErrNoSuchKey)
		}
		// Reports int/embstr/raw exactly as Redis would, which is the point
		// of packing type and encoding into the object header at all.
		return EncodeBulkString(obj.EncodingName())
	case "HELP":
		return EncodeStringArray([]string{"OBJECT ENCODING <key> -- internal encoding of the value"})
	default:
		return EncodeErrorf("ERR Unknown subcommand '%s'. Try OBJECT HELP.", args[0])
	}
}
