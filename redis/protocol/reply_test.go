package protocol

import "testing"

func TestIsErrorReplyHandlesEmptyReplies(t *testing.T) {
	if IsErrorReply(nil) {
		t.Fatal("nil reply is not an error")
	}
	if IsErrorReply(&NoReply{}) {
		t.Fatal("NoReply is not an error")
	}
	if !IsErrorReply(MakeErrReply("ERR boom")) {
		t.Fatal("error reply was not detected")
	}
	if IsErrorReply(MakeStatusReply("OK")) {
		t.Fatal("status reply was detected as an error")
	}
}
