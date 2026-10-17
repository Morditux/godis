package database

import "math"

const (
	// maxStringSize mirrors Redis's default proto-max-bulk-len (512 MiB).
	// It bounds the value size produced by SETRANGE/SETBIT/APPEND.
	maxStringSize = 512 << 20
	// maxBitOffset is Redis's maximum bitmap offset (2^32-1 bits).
	maxBitOffset = int64(maxStringSize) * 8

	// maxTTLSeconds/maxTTLMillis are the largest relative TTLs that fit in a
	// time.Duration (int64 nanoseconds). Larger values must be rejected
	// instead of silently wrapping into a negative TTL.
	maxTTLSeconds = math.MaxInt64 / 1_000_000_000
	maxTTLMillis  = math.MaxInt64 / 1_000_000

	// maxUnixSeconds/maxUnixMillis bound absolute timestamps so that
	// time.Unix(...).Sub(now) and UnixNano() do not overflow.
	maxUnixSeconds = math.MaxInt64 / 1_000_000_000
	maxUnixMillis  = math.MaxInt64 / 1_000_000
	minUnixSeconds = math.MinInt64 / 1_000_000_000
	minUnixMillis  = math.MinInt64 / 1_000_000
)

func invalidExpireErr(command string) string {
	return "ERR invalid expire time in " + command
}

// addInt64Overflow reports whether a+b would overflow int64.
func addInt64Overflow(a, b int64) bool {
	if b > 0 {
		return a > math.MaxInt64-b
	}
	if b < 0 {
		return a < math.MinInt64-b
	}
	return false
}

// subInt64Overflow reports whether a-b would overflow int64.
func subInt64Overflow(a, b int64) bool {
	if b > 0 {
		return a < math.MinInt64+b
	}
	if b < 0 {
		return a > math.MaxInt64+b
	}
	return false
}
