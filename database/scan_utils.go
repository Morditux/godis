package database

import (
	"strconv"
	"strings"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// parseScanOptions parses the MATCH/COUNT [/TYPE] options shared by SCAN,
// SSCAN, HSCAN and ZSCAN. It validates that every option has a value, which
// used to panic with an index-out-of-range on e.g. "SCAN 0 COUNT".
func parseScanOptions(args [][]byte, start int, allowType bool) (pattern string, count int, scanType string, errReply redis.Reply) {
	pattern = "*"
	count = 10
	for i := start; i < len(args); i++ {
		switch strings.ToLower(string(args[i])) {
		case "count":
			if i+1 >= len(args) {
				return "", 0, "", &protocol.SyntaxErrReply{}
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n <= 0 {
				return "", 0, "", &protocol.SyntaxErrReply{}
			}
			count = n
			i++
		case "match":
			if i+1 >= len(args) {
				return "", 0, "", &protocol.SyntaxErrReply{}
			}
			pattern = string(args[i+1])
			i++
		case "type":
			if !allowType || i+1 >= len(args) {
				return "", 0, "", &protocol.SyntaxErrReply{}
			}
			scanType = strings.ToLower(string(args[i+1]))
			i++
		default:
			return "", 0, "", &protocol.SyntaxErrReply{}
		}
	}
	return pattern, count, scanType, nil
}
