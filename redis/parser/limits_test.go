package parser

import (
	"strings"
	"testing"
)

func withLimits(t *testing.T, bulk int64, multi int64, inline int) {
	t.Helper()
	oldBulk, oldMulti, oldInline := MaxBulkLen, MaxMultiBulkLen, MaxInlineLen
	MaxBulkLen, MaxMultiBulkLen, MaxInlineLen = bulk, multi, inline
	t.Cleanup(func() {
		MaxBulkLen, MaxMultiBulkLen, MaxInlineLen = oldBulk, oldMulti, oldInline
	})
}

func TestBulkLengthLimit(t *testing.T) {
	withLimits(t, 16, 1024, 1024)
	if _, err := ParseOne([]byte("$17\r\n0123456789012345\r\n")); err == nil {
		t.Fatal("oversized bulk string was accepted")
	}
	// exactly at the limit is accepted
	if _, err := ParseOne([]byte("$16\r\n0123456789012345\r\n")); err != nil {
		t.Fatalf("bulk string at the limit was rejected: %v", err)
	}
}

func TestMultiBulkLengthLimit(t *testing.T) {
	withLimits(t, 1024, 2, 1024)
	if _, err := ParseOne([]byte("*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n")); err == nil {
		t.Fatal("oversized multi-bulk was accepted")
	}
	if _, err := ParseOne([]byte("*2\r\n$1\r\na\r\n$1\r\nb\r\n")); err != nil {
		t.Fatalf("multi-bulk at the limit was rejected: %v", err)
	}
}

func TestInlineLineLimit(t *testing.T) {
	withLimits(t, 16, 16, 8)
	if _, err := ParseOne([]byte("PING 0123456789\r\n")); err == nil {
		t.Fatal("oversized inline line was accepted")
	}
}

func TestMalformedLengthsDoNotPanic(t *testing.T) {
	cases := []string{
		"$abc\r\n",
		"$-2\r\n",
		"*abc\r\n",
		"*-1\r\n",
		"*2\r\n$abc\r\n",
		"*1\r\n+not-bulk\r\n",
	}
	for _, input := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("ParseOne(%q) panicked: %v", input, r)
				}
			}()
			_, _ = ParseOne([]byte(input))
		}()
	}
}

func TestReadLineBounded(t *testing.T) {
	withLimits(t, 1024, 1024, 8)
	if _, err := ParseOne([]byte(strings.Repeat("a", 20) + "\r\n")); err == nil {
		t.Fatal("long line accepted")
	}
}
