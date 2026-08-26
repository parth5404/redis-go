package core

import (
	"strconv"
	"sync/atomic"
	"time"
)

// Obj is the single value wrapper stored in the keyspace, modelled on Redis's
// robj. Type and encoding are bit-packed into one byte: the high nibble is the
// type, the low nibble is the encoding. One byte instead of two saves 8 bytes
// per object after Go's struct alignment, which matters at a million keys.
type Obj struct {
	// Value holds the concrete payload. For OBJ_TYPE_STRING this is always
	// a string, regardless of encoding -- the encoding is a hint about the
	// *shape* of the data (used by OBJECT ENCODING and by INCR to reject
	// non-numeric values cheaply), not a different in-memory representation.
	Value interface{}

	// ExpiresAt is an absolute Unix millisecond timestamp, or NoExpiry.
	// Absolute rather than relative so that it survives an AOF round-trip
	// and so expiry checks are a single comparison with no arithmetic.
	ExpiresAt int64

	// lastAccess is an absolute Unix millisecond timestamp of the last read
	// or write, used by approximated-LRU eviction.
	//
	// It is read and written with sync/atomic because Get() updates it while
	// holding only a *read* lock. That is deliberate: taking the write lock
	// on every read would serialise all readers and defeat the RWMutex. An
	// atomic store is safe under a read lock and is race-detector clean.
	lastAccess int64

	// typeEncoding is the packed type|encoding byte: high nibble type, low
	// nibble encoding. Unexported so the two halves can never be set to an
	// inconsistent pair from outside this package.
	typeEncoding uint8
}

// NoExpiry marks a key that never expires.
const NoExpiry int64 = -1

// Object types occupy the high nibble of TypeEncoding.
const (
	ObjTypeString uint8 = 0 << 4
)

// Object encodings occupy the low nibble of TypeEncoding.
const (
	// ObjEncodingRaw is a plain string longer than embstrLimit.
	ObjEncodingRaw uint8 = 0
	// ObjEncodingInt is a string that parses as a 64-bit integer.
	ObjEncodingInt uint8 = 1
	// ObjEncodingEmbStr is a short string. Real Redis allocates these
	// inline with the robj header; we only track the distinction so that
	// OBJECT ENCODING reports the same thing Redis would.
	ObjEncodingEmbStr uint8 = 8
)

// embstrLimit is Redis's OBJ_ENCODING_EMBSTR_SIZE_LIMIT.
const embstrLimit = 44

// TypeEncoding is the packed type|encoding byte.
//
// Exposed as a method rather than a field so the two nibbles can never be set
// to an inconsistent pair from outside the package.
func (o *Obj) TypeEncoding() uint8 { return o.typeEncoding }

// Type returns the high nibble.
func (o *Obj) Type() uint8 { return o.typeEncoding & 0xF0 }

// Encoding returns the low nibble.
func (o *Obj) Encoding() uint8 { return o.typeEncoding & 0x0F }

// EncodingName returns the string OBJECT ENCODING reports.
func (o *Obj) EncodingName() string {
	switch o.Encoding() {
	case ObjEncodingInt:
		return "int"
	case ObjEncodingEmbStr:
		return "embstr"
	default:
		return "raw"
	}
}

// TypeName returns the string TYPE reports.
func (o *Obj) TypeName() string {
	switch o.Type() {
	case ObjTypeString:
		return "string"
	default:
		return "none"
	}
}

// Touch records an access for LRU purposes. Safe to call under a read lock.
func (o *Obj) Touch(nowMs int64) { atomic.StoreInt64(&o.lastAccess, nowMs) }

// LastAccess reads the LRU timestamp. Safe to call under a read lock.
func (o *Obj) LastAccess() int64 { return atomic.LoadInt64(&o.lastAccess) }

// IsExpired reports whether the object's TTL has elapsed as of nowMs.
func (o *Obj) IsExpired(nowMs int64) bool {
	return o.ExpiresAt != NoExpiry && nowMs >= o.ExpiresAt
}

// StringValue returns the payload as a string. Only valid for ObjTypeString,
// which is currently the only type.
func (o *Obj) StringValue() string {
	s, _ := o.Value.(string)
	return s
}

// NewStringObj builds a string object, deducing its encoding from the value.
//
// expiresAt is an absolute Unix millisecond timestamp or NoExpiry. Callers
// that have a *duration* should convert it with ExpiryFromDuration first; this
// constructor deliberately does not accept durations, because the original
// code's "durationMs > 0 means set an expiry" rule silently swallowed both
// `EX 0` (which Redis rejects) and negative durations (which Redis treats as
// an immediate delete).
func NewStringObj(value string, expiresAt int64) *Obj {
	now := time.Now().UnixMilli()
	return &Obj{
		Value:        value,
		ExpiresAt:    expiresAt,
		lastAccess:   now,
		typeEncoding: ObjTypeString | deduceStringEncoding(value),
	}
}

// deduceStringEncoding mirrors Redis's tryObjectEncoding.
func deduceStringEncoding(val string) uint8 {
	// Redis only uses the int encoding for values up to 20 digits, which is
	// exactly what ParseInt's 64-bit range enforces.
	if _, err := strconv.ParseInt(val, 10, 64); err == nil {
		return ObjEncodingInt
	}
	if len(val) <= embstrLimit {
		return ObjEncodingEmbStr
	}
	return ObjEncodingRaw
}

// setStringValue replaces the payload and re-derives the encoding. The caller
// must hold the owning shard's write lock.
func (o *Obj) setStringValue(val string) {
	o.Value = val
	o.typeEncoding = ObjTypeString | deduceStringEncoding(val)
}

// ExpiryFromDuration converts a relative millisecond duration into the
// absolute timestamp stored in Obj.ExpiresAt.
func ExpiryFromDuration(ms int64) int64 { return time.Now().UnixMilli() + ms }
