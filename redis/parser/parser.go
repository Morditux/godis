package parser

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/protocol"
)

// Payload stores redis.Reply or error
type Payload struct {
	Data redis.Reply
	Err  error
}

// ParseStream reads data from io.Reader and send payloads through channel.
// The returned channel is closed when the internal goroutine exits:
//   - the reader reached EOF or returned an error,
//   - stop was closed and no in-flight payload remains to deliver.
//
// Note that closing stop cannot interrupt a blocked read: callers must also
// close (or renew) the underlying connection, e.g. by closing net.Conn.
func ParseStream(reader io.Reader) <-chan *Payload {
	return ParseStreamWithStop(reader, nil)
}

// ParseStreamWithStop behaves like ParseStream and accepts a stop channel.
// Passing nil is equivalent to ParseStream.
func ParseStreamWithStop(reader io.Reader, stop <-chan struct{}) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch, stop)
	return ch
}

// send0 delivers a payload to the channel or gives up when stop is closed.
// A nil stop channel never becomes ready, which allows ParseStream to keep
// its original blocking behavior.
func send0(ch chan<- *Payload, stop <-chan struct{}, p *Payload) bool {
	select {
	case ch <- p:
		return true
	case <-stop:
		return false
	}
}

// ParseBytes reads data from []byte and return all replies
func ParseBytes(data []byte) ([]redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch, nil)
	var results []redis.Reply
	for payload := range ch {
		if payload == nil {
			return nil, errors.New("no protocol")
		}
		if payload.Err != nil {
			if errors.Is(payload.Err, io.EOF) {
				break
			}
			return nil, payload.Err
		}
		results = append(results, payload.Data)
	}
	return results, nil
}

// ParseOne reads data from []byte and return the first payload
// Parse the whole data (until EOF) so that the internal parse goroutine
// can always terminate, otherwise ParseOne would leak a goroutine per call
// when data contains more than one payload.
func ParseOne(data []byte) (redis.Reply, error) {
	results, err := ParseBytes(data)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, errors.New("no protocol")
	}
	return results[0], nil
}

func parse0(rawReader io.Reader, ch chan<- *Payload, stop <-chan struct{}) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err, string(debug.Stack()))
		}
		close(ch)
	}()
	reader := bufio.NewReader(rawReader)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			// reader error/EOF: deliver it and terminate, closing the channel
			_ = send0(ch, stop, &Payload{Err: err})
			return
		}
		length := len(line)
		if length <= 2 || line[length-2] != '\r' {
			// there are some empty lines within replication traffic, ignore this error
			//protocolError(ch, "empty line")
			continue
		}
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
		switch line[0] {
		case '+':
			content := string(line[1:])
			if !send0(ch, stop, &Payload{Data: protocol.MakeStatusReply(content)}) {
				return
			}
			if strings.HasPrefix(content, "FULLRESYNC") {
				err = parseRDBBulkString(reader, ch, stop)
				if err != nil {
					_ = send0(ch, stop, &Payload{Err: err})
					return
				}
			}
		case '-':
			if !send0(ch, stop, &Payload{Data: protocol.MakeErrReply(string(line[1:]))}) {
				return
			}
		case ':':
			value, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				protocolError(ch, stop, "illegal number "+string(line[1:]))
				continue
			}
			if !send0(ch, stop, &Payload{Data: protocol.MakeIntReply(value)}) {
				return
			}
		case '$':
			err = parseBulkString(line, reader, ch, stop)
			if err != nil {
				_ = send0(ch, stop, &Payload{Err: err})
				return
			}
		case '*':
			err = parseArray(line, reader, ch, stop)
			if err != nil {
				_ = send0(ch, stop, &Payload{Err: err})
				return
			}
		default:
			args := bytes.Split(line, []byte{' '})
			if !send0(ch, stop, &Payload{Data: protocol.MakeMultiBulkReply(args)}) {
				return
			}
		}
	}
}

func parseBulkString(header []byte, reader *bufio.Reader, ch chan<- *Payload, stop <-chan struct{}) error {
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strLen < -1 {
		protocolError(ch, stop, "illegal bulk string header: "+string(header))
		return nil
	} else if strLen == -1 {
		if !send0(ch, stop, &Payload{Data: protocol.MakeNullBulkReply()}) {
			return nil
		}
		return nil
	}
	body := make([]byte, strLen+2)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	if !send0(ch, stop, &Payload{Data: protocol.MakeBulkReply(body[:len(body)-2])}) {
		return nil
	}
	return nil
}

// there is no CRLF between RDB and following AOF, therefore it needs to be treated differently
func parseRDBBulkString(reader *bufio.Reader, ch chan<- *Payload, stop <-chan struct{}) error {
	header, err := reader.ReadBytes('\n')
	if err != nil {
		return errors.New("failed to read bytes")
	}
	header = bytes.TrimSuffix(header, []byte{'\r', '\n'})
	if len(header) == 0 {
		return errors.New("empty header")
	}
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strLen <= 0 {
		return errors.New("illegal bulk header: " + string(header))
	}
	body := make([]byte, strLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	if !send0(ch, stop, &Payload{Data: protocol.MakeBulkReply(body[:len(body)])}) {
		return nil
	}
	return nil
}

func parseArray(header []byte, reader *bufio.Reader, ch chan<- *Payload, stop <-chan struct{}) error {
	nStrs, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || nStrs < 0 {
		protocolError(ch, stop, "illegal array header "+string(header[1:]))
		return nil
	} else if nStrs == 0 {
		if !send0(ch, stop, &Payload{Data: protocol.MakeEmptyMultiBulkReply()}) {
			return nil
		}
		return nil
	}
	lines := make([][]byte, 0, nStrs)
	for i := int64(0); i < nStrs; i++ {
		var line []byte
		line, err = reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		length := len(line)
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			protocolError(ch, stop, "illegal bulk string header "+string(line))
			break
		}
		strLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		if err != nil || strLen < -1 {
			protocolError(ch, stop, "illegal bulk string length "+string(line))
			break
		} else if strLen == -1 {
			lines = append(lines, []byte{})
		} else {
			body := make([]byte, strLen+2)
			_, err := io.ReadFull(reader, body)
			if err != nil {
				return err
			}
			lines = append(lines, body[:len(body)-2])
		}
	}
	if !send0(ch, stop, &Payload{Data: protocol.MakeMultiBulkReply(lines)}) {
		return nil
	}
	return nil
}

func protocolError(ch chan<- *Payload, stop <-chan struct{}, msg string) {
	err := errors.New("protocol error: " + msg)
	_ = send0(ch, stop, &Payload{Err: err})
}
