package core

import (
	"strings"
	"time"
)

// setOptions holds the parsed tail of a SET command.
type setOptions struct {
	// expiresAt is an absolute Unix ms timestamp, or NoExpiry.
	expiresAt int64
	// hasExpiry distinguishes "no expiry option given" from "EX supplied".
	hasExpiry bool
	nx, xx    bool
	keepTTL   bool
	get       bool
}

// parseSetOptions parses SET's optional arguments.
//
// The original loop only understood EX and used `i++` inside a `for i` loop to
// consume the argument, which reads correctly but silently accepted `SET k v EX`
// with the value missing on some paths. It also computed a *relative* duration
// and stored `expMs = seconds * 1000`, with NewObj interpreting "greater than
// zero" as "has an expiry" -- so `EX 0` and `EX -1`, both of which Redis
// rejects, quietly created a key with no expiry at all.
func parseSetOptions(args []string) (*setOptions, error) {
	opts := &setOptions{expiresAt: NoExpiry}
	now := time.Now().UnixMilli()

	for i := 0; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "EX", "PX", "EXAT", "PXAT":
			kind := strings.ToUpper(args[i])
			if opts.hasExpiry || opts.keepTTL {
				return nil, ErrSyntax
			}
			if i+1 >= len(args) {
				return nil, ErrSyntax
			}
			i++
			n, err := parseInt64(args[i])
			if err != nil {
				return nil, ErrNotInteger
			}
			// Redis rejects a non-positive relative expiry outright rather
			// than treating it as "never expires".
			if (kind == "EX" || kind == "PX") && n <= 0 {
				return nil, ErrInvalidExpire
			}
			switch kind {
			case "EX":
				// Guard the multiplication: EX 9223372036854775 would
				// overflow int64 milliseconds and produce a timestamp in
				// the past, expiring the key immediately.
				if n > (maxInt64-now)/1000 {
					return nil, ErrInvalidExpire
				}
				opts.expiresAt = now + n*1000
			case "PX":
				if n > maxInt64-now {
					return nil, ErrInvalidExpire
				}
				opts.expiresAt = now + n
			case "EXAT":
				if n > maxInt64/1000 {
					return nil, ErrInvalidExpire
				}
				opts.expiresAt = n * 1000
			case "PXAT":
				opts.expiresAt = n
			}
			opts.hasExpiry = true

		case "NX":
			if opts.xx {
				return nil, ErrSyntax
			}
			opts.nx = true
		case "XX":
			if opts.nx {
				return nil, ErrSyntax
			}
			opts.xx = true
		case "KEEPTTL":
			if opts.hasExpiry {
				return nil, ErrSyntax
			}
			opts.keepTTL = true
		case "GET":
			opts.get = true
		default:
			return nil, ErrSyntax
		}
	}
	return opts, nil
}

func evalSET(args []string) []byte {
	key, value := args[0], args[1]
	opts, err := parseSetOptions(args[2:])
	if err != nil {
		return EncodeError(err)
	}

	existing := KV.Get(key)

	// Capture the old value before any mutation, for the GET option.
	var oldReply []byte
	if opts.get {
		if existing == nil {
			oldReply = RespNil
		} else if existing.Type() != ObjTypeString {
			// SET ... GET must fail rather than clobber a non-string.
			return EncodeError(ErrWrongType)
		} else {
			oldReply = EncodeBulkString(existing.StringValue())
		}
	}

	if opts.nx && existing != nil {
		if opts.get {
			return oldReply
		}
		return RespNil
	}
	if opts.xx && existing == nil {
		if opts.get {
			return oldReply
		}
		return RespNil
	}

	expiresAt := opts.expiresAt
	if opts.keepTTL && existing != nil {
		expiresAt = existing.ExpiresAt
	}

	if !KV.Put(key, NewStringObj(value, expiresAt)) {
		return EncodeError(ErrOOM)
	}
	if opts.get {
		return oldReply
	}
	return RespOK
}

func evalSETNX(args []string) []byte {
	key, value := args[0], args[1]
	if KV.Exists(key) {
		return EncodeInt(0)
	}
	if !KV.Put(key, NewStringObj(value, NoExpiry)) {
		return EncodeError(ErrOOM)
	}
	return EncodeInt(1)
}

func evalGET(args []string) []byte {
	obj := KV.Get(args[0])
	if obj == nil {
		return RespNil
	}
	if obj.Type() != ObjTypeString {
		return EncodeError(ErrWrongType)
	}
	return EncodeBulkString(obj.StringValue())
}

func evalGETSET(args []string) []byte {
	key, value := args[0], args[1]
	old := KV.Get(key)
	if old != nil && old.Type() != ObjTypeString {
		return EncodeError(ErrWrongType)
	}
	// GETSET clears any existing TTL, matching Redis.
	if !KV.Put(key, NewStringObj(value, NoExpiry)) {
		return EncodeError(ErrOOM)
	}
	if old == nil {
		return RespNil
	}
	return EncodeBulkString(old.StringValue())
}

func evalGETDEL(args []string) []byte {
	key := args[0]
	obj := KV.Get(key)
	if obj == nil {
		return RespNil
	}
	if obj.Type() != ObjTypeString {
		return EncodeError(ErrWrongType)
	}
	val := obj.StringValue()
	KV.Del(key)
	return EncodeBulkString(val)
}

func evalMSET(args []string) []byte {
	// MSET requires whole key/value pairs; an odd count is an arity error,
	// not a partial write.
	if len(args)%2 != 0 {
		return EncodeError(wrongArity("mset"))
	}
	for i := 0; i < len(args); i += 2 {
		if !KV.Put(args[i], NewStringObj(args[i+1], NoExpiry)) {
			return EncodeError(ErrOOM)
		}
	}
	return RespOK
}

func evalMGET(args []string) []byte {
	out := make([]*string, len(args))
	for i, k := range args {
		obj := KV.Get(k)
		// Redis returns a null element for a missing key *and* for a key
		// holding the wrong type, rather than failing the whole command.
		if obj == nil || obj.Type() != ObjTypeString {
			out[i] = nil
			continue
		}
		v := obj.StringValue()
		out[i] = &v
	}
	return EncodeNullableStringArray(out)
}

// incrBy is the shared implementation of INCR/DECR/INCRBY/DECRBY.
//
// All four delegate to Store.Increment, which performs the read-modify-write
// under a single write lock. Doing the arithmetic in the handler -- reading via
// Get, computing, then writing back -- would reintroduce the lost-update race
// even though each individual call is locked.
func incrBy(key string, delta int64) []byte {
	next, err := KV.Increment(key, delta)
	if err != nil {
		return EncodeError(err)
	}
	return EncodeInt(next)
}

func evalINCR(args []string) []byte { return incrBy(args[0], 1) }
func evalDECR(args []string) []byte { return incrBy(args[0], -1) }

func evalINCRBY(args []string) []byte {
	delta, err := parseInt64(args[1])
	if err != nil {
		return EncodeError(ErrNotInteger)
	}
	return incrBy(args[0], delta)
}

func evalDECRBY(args []string) []byte {
	delta, err := parseInt64(args[1])
	if err != nil {
		return EncodeError(ErrNotInteger)
	}
	// Negating minInt64 overflows, so reject it rather than wrapping to
	// itself and silently decrementing in the wrong direction.
	if delta == minInt64 {
		return EncodeError(ErrIncrOverflow)
	}
	return incrBy(args[0], -delta)
}

func evalAPPEND(args []string) []byte {
	key, suffix := args[0], args[1]
	n, err := KV.Append(key, suffix)
	if err != nil {
		return EncodeError(err)
	}
	return EncodeInt(n)
}

func evalSTRLEN(args []string) []byte {
	obj := KV.Get(args[0])
	if obj == nil {
		return EncodeInt(0)
	}
	if obj.Type() != ObjTypeString {
		return EncodeError(ErrWrongType)
	}
	return EncodeInt(int64(len(obj.StringValue())))
}
