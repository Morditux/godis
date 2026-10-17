package gnet

import (
	"errors"
	"strconv"

	"github.com/panjf2000/gnet/v2"
)

const (
	// protoMaxBulkLen mirrors redis's proto-max-bulk-len default (512 MB).
	// The parser allocates the bulk body upfront and the gnet connection
	// buffers it in memory, so a far lower cap than redis's is also viable.
	protoMaxBulkLen = 512 << 20
	// maxCommandArgs mirrors redis's proto-max-multibulk default (1M arguments).
	maxCommandArgs = 1 << 20
)

var (
	errBadHeader    = errors.New("bad RESP header")
	errTooManyArgs  = errors.New("too many arguments")
	errBulkTooLarge = errors.New("bulk length exceeds limit")
)

// scanCommand inspects the buffered bytes and reports whether a complete
// command (RESP multi-bulk array or inline command) is present, and how many
// bytes it occupies. It replicates the semantics of parser.ParseV2 without
// consuming anything, so the caller only parses when the whole command is
// already buffered:
//   - (size, true, nil): complete command of exactly size bytes,
//     parser.ParseV2 will not hit an incomplete read
//   - (0, false, nil):   the command depends on bytes that have not arrived
//     yet; the caller should keep the buffer untouched and wait
//   - (0, false, err):   malformed data the parser rejects, the caller should
//     close the connection
func scanCommand(data []byte) (int, bool, error) {
	if len(data) == 0 {
		return 0, false, nil
	}
	if data[0] != '*' {
		// inline command, terminated by the first \r\n
		for i := 0; i < len(data); i++ {
			if data[i] == '\r' {
				if i+1 >= len(data) {
					return 0, false, nil // need more bytes to decide
				}
				if data[i+1] != '\n' {
					return 0, false, errors.New("expected \\n after \\r")
				}
				return i + 2, true, nil
			}
		}
		return 0, false, nil
	}

	// multi bulk: *count\r\n then args as $len\r\n<data>\r\n
	argv, next, ok, err := parseIntHeader(data, 1)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil
	}
	if argv > maxCommandArgs {
		return 0, false, errTooManyArgs
	}
	if argv < 0 {
		// null array, ParseV2 returns a nil reply, only the header is consumed
		return next, true, nil
	}
	for argv > 0 {
		if next >= len(data) {
			return 0, false, nil
		}
		if data[next] != '$' {
			return 0, false, errors.New("unsupported RESP type")
		}
		argLen, next2, ok, err := parseIntHeader(data, next+1)
		if err != nil {
			return 0, false, err
		}
		if !ok {
			return 0, false, nil
		}
		if argLen < 0 {
			// null bulk string, only the header is consumed
			next = next2
			argv--
			continue
		}
		if argLen > protoMaxBulkLen {
			return 0, false, errBulkTooLarge
		}
		total := next2 + argLen + 2 // body (argLen bytes) + trailing CRLF
		if total > len(data) {
			return total, false, nil
		}
		next = total
		argv--
	}
	return next, true, nil
}

// parseIntHeader reads a decimal integer terminated by \r\n in data[from:]:
// it returns (value, index after \r\n, true) when the terminating CRLF is
// within data, (0, 0, false) when more bytes are needed, or an error on
// malformed/overflowing input (mirroring strconv.Atoi failures).
func parseIntHeader(data []byte, from int) (int, int, bool, error) {
	end := from
	for end < len(data) {
		if data[end] == '\r' {
			if end+1 >= len(data) {
				return 0, 0, false, nil
			}
			if data[end+1] != '\n' {
				return 0, 0, false, errors.New("expected \\n after \\r")
			}
			value, err := parseDecimal(data[from:end])
			if err != nil {
				return 0, 0, false, err
			}
			if value > protoMaxBulkLen {
				return 0, 0, false, errBulkTooLarge
			}
			return value, end + 2, true, nil
		}
		end++
	}
	return 0, 0, false, nil
}

// parseDecimal parses a signed decimal integer, mimicking strconv.Atoi
// (including errors on empty input, non-digits and overflow) without
// allocating a string for the bytes slice.
func parseDecimal(data []byte) (int, error) {
	neg := false
	i := 0
	if len(data) > 0 && (data[0] == '-' || data[0] == '+') {
		neg = data[0] == '-'
		i = 1
	}
	if i == len(data) {
		return 0, strconv.ErrSyntax
	}
	value := 0
	for ; i < len(data); i++ {
		c := data[i]
		if c < '0' || c > '9' {
			return 0, strconv.ErrSyntax
		}
		value = value*10 + int(c-'0')
		if value > protoMaxBulkLen {
			return 0, strconv.ErrRange
		}
	}
	if neg {
		value = -value
	}
	return value, nil
}

// estimateCommandLength applies scanCommand to the bytes currently buffered
// by the gnet connection (without consuming them).
func estimateCommandLength(c gnet.Conn) (int, bool, error) {
	buffered := c.InboundBuffered()
	if buffered <= 0 {
		return 0, false, nil
	}
	data, err := c.Peek(buffered)
	if err != nil {
		// Peek can only fail with ErrShortBuffer when fewer bytes are
		// available than requested, which is impossible here: wait anyway
		return 0, false, nil
	}
	return scanCommand(data)
}
