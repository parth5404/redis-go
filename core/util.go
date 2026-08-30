package core

import (
	"errors"
	"math"
	"strconv"
)

// Error replies. Each string matches real Redis byte-for-byte where an
// equivalent error exists, so a client written against Redis sees what it
// expects. The `ERR`/`WRONGTYPE` prefix is part of the protocol: clients parse
// it to classify failures.
var (
	ErrWrongType     = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrNotInteger    = errors.New("ERR value is not an integer or out of range")
	ErrIncrOverflow  = errors.New("ERR increment or decrement would overflow")
	ErrSyntax        = errors.New("ERR syntax error")
	ErrOOM           = errors.New("OOM command not allowed when used memory > 'maxmemory'")
	ErrInvalidExpire = errors.New("ERR invalid expire time")
	ErrNoSuchKey     = errors.New("ERR no such key")
)

const (
	maxInt64 = math.MaxInt64
	minInt64 = math.MinInt64
)

func parseInt64(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) }

func formatInt64(v int64) string { return strconv.FormatInt(v, 10) }

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// wrongArity builds Redis's arity error for cmd.
func wrongArity(cmd string) error {
	return errors.New("ERR wrong number of arguments for '" + cmd + "' command")
}

// matchGlob reports whether name matches a Redis-style glob pattern.
//
// This is a port of Redis's stringmatchlen. It supports:
//
//   - any sequence, including empty
//     ?        exactly one character
//     [abc]    any one of a, b, c
//     [^abc]   any character except a, b, c
//     [a-z]    any character in the range
//     \x       a literal x
//
// Implemented iteratively with a backtrack point rather than recursively:
// recursion on `*` makes a pattern like "*a*a*a*a*b" against a long string of
// 'a's take exponential time, which is a trivial remote CPU exhaustion via
// `KEYS`. The two-pointer form is O(len(pattern) * len(name)) worst case.
func matchGlob(pattern, name string) bool {
	// Fast path: `*` matches everything and is by far the most common
	// pattern (KEYS *), so skip the machinery entirely.
	if pattern == "*" {
		return true
	}

	var (
		p, n         int
		starP, starN = -1, 0
	)
	for n < len(name) {
		switch {
		case p < len(pattern) && pattern[p] == '*':
			// Record where to resume if the rest of the pattern fails to
			// match, then optimistically let `*` consume nothing.
			starP, starN = p, n
			p++
		case p < len(pattern) && matchGlobClass(pattern, &p, name[n]):
			n++
		case starP >= 0:
			// Backtrack: let the last `*` swallow one more character.
			starN++
			n = starN
			p = starP + 1
		default:
			return false
		}
	}
	// Trailing stars can match the empty remainder.
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}

// matchGlobClass tests one pattern element against one byte, advancing p past
// the element it consumed. Returns false without advancing on no match.
func matchGlobClass(pattern string, p *int, c byte) bool {
	i := *p
	switch pattern[i] {
	case '?':
		*p = i + 1
		return true

	case '[':
		// Scan to the closing bracket first. An unterminated class is
		// treated as a literal '[' by Redis, so mirror that.
		j := i + 1
		negate := false
		if j < len(pattern) && (pattern[j] == '^') {
			negate = true
			j++
		}
		matched := false
		closed := false
		for j < len(pattern) {
			if pattern[j] == '\\' && j+1 < len(pattern) {
				j++
				if pattern[j] == c {
					matched = true
				}
				j++
				continue
			}
			if pattern[j] == ']' {
				closed = true
				j++
				break
			}
			// Range form a-z, but only when '-' is not the last element.
			if j+2 < len(pattern) && pattern[j+1] == '-' && pattern[j+2] != ']' {
				lo, hi := pattern[j], pattern[j+2]
				if lo > hi {
					lo, hi = hi, lo
				}
				if c >= lo && c <= hi {
					matched = true
				}
				j += 3
				continue
			}
			if pattern[j] == c {
				matched = true
			}
			j++
		}
		if !closed {
			// Unterminated: literal '['.
			if c == '[' {
				*p = i + 1
				return true
			}
			return false
		}
		if negate {
			matched = !matched
		}
		if matched {
			*p = j
			return true
		}
		return false

	case '\\':
		if i+1 < len(pattern) {
			if pattern[i+1] == c {
				*p = i + 2
				return true
			}
			return false
		}
		// Trailing backslash: literal.
		if c == '\\' {
			*p = i + 1
			return true
		}
		return false

	default:
		if pattern[i] == c {
			*p = i + 1
			return true
		}
		return false
	}
}
