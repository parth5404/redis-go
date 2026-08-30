package core

import (
	"strings"
	"time"
)

// AOF command rewriting.
//
// Replaying a log must reproduce the state the log recorded. Any command whose
// effect depends on *when* it runs breaks that guarantee, and every relative
// expiry is such a command: `SET k v EX 10` means "ten seconds from now", and
// "now" during replay is not the "now" at which it was first executed.
//
// The fix, which Redis applies too, is to translate relative deadlines into
// absolute ones before they reach the log.
//
// The absolute timestamp is recomputed here rather than threaded out of the
// handler, so it lands a few microseconds later than the value actually stored.
// That skew is bounded by the time between executing a command and propagating
// it -- well under a millisecond -- and is irrelevant against any real TTL.

// rewriteSet converts SET's relative expiry options into PXAT, and drops
// options that are meaningless on replay.
func rewriteSet(args []string) *RedisCmd {
	if len(args) < 2 {
		return &RedisCmd{Cmd: "SET", Args: args}
	}
	out := make([]string, 0, len(args))
	out = append(out, args[0], args[1])

	now := time.Now().UnixMilli()
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "EX", "PX", "EXAT", "PXAT":
			kind := strings.ToUpper(args[i])
			if i+1 >= len(args) {
				return &RedisCmd{Cmd: "SET", Args: args}
			}
			n, err := parseInt64(args[i+1])
			if err != nil {
				return &RedisCmd{Cmd: "SET", Args: args}
			}
			i++
			var abs int64
			switch kind {
			case "EX":
				abs = now + n*1000
			case "PX":
				abs = now + n
			case "EXAT":
				abs = n * 1000
			case "PXAT":
				abs = n
			}
			out = append(out, "PXAT", formatInt64(abs))

		case "GET":
			// GET only shapes the *reply*, never the stored value. Keeping
			// it in the log would be harmless but misleading.

		case "NX", "XX":
			// Deliberately preserved. These are deterministic on replay:
			// the log is applied in the original order, so the key's
			// presence at that point in the log matches what it was when
			// the command first ran. Dropping them would turn a conditional
			// write into an unconditional one.
			out = append(out, strings.ToUpper(args[i]))

		case "KEEPTTL":
			out = append(out, "KEEPTTL")

		default:
			out = append(out, args[i])
		}
	}
	return &RedisCmd{Cmd: "SET", Args: out}
}

// rewriteExpire converts EXPIRE/PEXPIRE into PEXPIREAT with an absolute
// deadline. unitMs is the multiplier for the incoming value.
func rewriteExpire(unitMs int64) func([]string) *RedisCmd {
	name := "PEXPIRE"
	if unitMs == 1000 {
		name = "EXPIRE"
	}
	return func(args []string) *RedisCmd {
		if len(args) < 2 {
			return &RedisCmd{Cmd: name, Args: args}
		}
		n, err := parseInt64(args[1])
		if err != nil {
			return &RedisCmd{Cmd: name, Args: args}
		}
		// A non-positive TTL deletes the key. Record that effect directly
		// rather than an expiry timestamp in the past, which replay would
		// have to interpret.
		if n <= 0 {
			return &RedisCmd{Cmd: "DEL", Args: []string{args[0]}}
		}
		abs := time.Now().UnixMilli() + n*unitMs
		out := []string{args[0], formatInt64(abs)}
		// Preserve NX/XX/GT/LT: they are evaluated against the key's state,
		// which replay reproduces in order.
		out = append(out, args[2:]...)
		return &RedisCmd{Cmd: "PEXPIREAT", Args: out}
	}
}

// expireAtCommon implements EXPIREAT and PEXPIREAT.
func expireAtCommon(args []string, unitMs int64) []byte {
	key := args[0]
	n, err := parseInt64(args[1])
	if err != nil {
		return EncodeError(ErrNotInteger)
	}
	if unitMs != 1 && (n > maxInt64/unitMs || n < minInt64/unitMs) {
		return EncodeError(ErrInvalidExpire)
	}
	deadline := n * unitMs

	obj := KV.Get(key)
	if obj == nil {
		return EncodeInt(0)
	}
	// A deadline already in the past deletes the key immediately.
	if deadline <= time.Now().UnixMilli() {
		KV.Del(key)
		return EncodeInt(1)
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
	if (nx && (xx || gt || lt)) || (gt && lt) {
		return EncodeErrorf("ERR NX and XX, GT or LT options at the same time are not compatible")
	}

	current := obj.ExpiresAt
	switch {
	case nx && current != NoExpiry:
		return EncodeInt(0)
	case xx && current == NoExpiry:
		return EncodeInt(0)
	case gt && (current == NoExpiry || deadline <= current):
		return EncodeInt(0)
	case lt && current != NoExpiry && deadline >= current:
		return EncodeInt(0)
	}

	if !KV.SetExpiry(key, deadline) {
		return EncodeInt(0)
	}
	return EncodeInt(1)
}

func evalEXPIREAT(args []string) []byte  { return expireAtCommon(args, 1000) }
func evalPEXPIREAT(args []string) []byte { return expireAtCommon(args, 1) }
