package pubsub

import (
	"strings"
	"testing"

	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
)

func TestSubscribeAndPublish(t *testing.T) {
	hub := MakeHub()
	c := connection.NewFakeConn()

	// subscribe writes confirmation message
	res := Subscribe(hub, c, utils.ToCmdLine("ch1"))
	if res == nil {
		t.Fatal("expected nil reply for subscribe, got reply")
	}
	confirm := c.Bytes()
	if !strings.Contains(string(confirm), "$9\r\nsubscribe\r\n$3\r\nch1\r\n:1") {
		t.Errorf("expected subscribe confirmation, got: %s", string(confirm))
	}

	// resubscribe to the same channel does not rewrite confirmation
	c.Clean()
	_ = Subscribe(hub, c, utils.ToCmdLine("ch1"))
	if len(c.Bytes()) != 0 {
		t.Errorf("unexpected message on duplicate subscribe: %s", string(c.Bytes()))
	}

	// publish reaches the subscriber
	c.Clean()
	res = Publish(hub, utils.ToCmdLine("ch1", "msg"))
	intReply, ok := res.(*protocol.IntReply)
	if !ok || intReply.Code != 1 {
		t.Errorf("expected 1 subscriber, got %v", res)
	}
	delivered := string(c.Bytes())
	if !strings.Contains(delivered, "$3\r\nch1\r\n$3\r\nmsg\r\n") {
		t.Errorf("expected message delivery, got: %s", delivered)
	}
	if !strings.Contains(delivered, "$7\r\nmessage\r\n") {
		t.Errorf("expected message type, got: %s", delivered)
	}
}

func TestPublishWithoutSubscriber(t *testing.T) {
	hub := MakeHub()
	res := Publish(hub, utils.ToCmdLine("empty", "msg"))
	intReply, ok := res.(*protocol.IntReply)
	if !ok || intReply.Code != 0 {
		t.Errorf("expected 0 subscriber, got %v", res)
	}
}

func TestPublishToMultipleSubscribers(t *testing.T) {
	hub := MakeHub()
	c1 := connection.NewFakeConn()
	c2 := connection.NewFakeConn()
	_ = Subscribe(hub, c1, utils.ToCmdLine("ch"))
	_ = Subscribe(hub, c2, utils.ToCmdLine("ch"))

	c1.Clean()
	c2.Clean()
	res := Publish(hub, utils.ToCmdLine("ch", "hello"))
	intReply, ok := res.(*protocol.IntReply)
	if !ok || intReply.Code != 2 {
		t.Errorf("expected 2 subscribers, got %v", res)
	}
	for _, c := range []string{string(c1.Bytes()), string(c2.Bytes())} {
		if !strings.Contains(c, "$2\r\nch\r\n$5\r\nhello\r\n") {
			t.Errorf("expected delivery containing channel and message, got: %q", c)
		}
	}
}

func TestUnsubscribe(t *testing.T) {
	hub := MakeHub()
	c := connection.NewFakeConn()
	c2 := connection.NewFakeConn()
	_ = Subscribe(hub, c, utils.ToCmdLine("ch1"))
	_ = Subscribe(hub, c2, utils.ToCmdLine("ch1"))

	// unsubscribe one subscriber
	c.Clean()
	res := UnSubscribe(hub, c, utils.ToCmdLine("ch1"))
	if res == nil {
		t.Fatal("expected nil reply for unsubscribe, got reply")
	}
	msg := string(c.Bytes())
	if !strings.Contains(msg, "$11\r\nunsubscribe\r\n$3\r\nch1\r\n") {
		t.Errorf("expected unsubscribe confirmation, got: %s", msg)
	}

	// after unsubscribe, publish should only reach c2
	c2.Clean()
	res = Publish(hub, utils.ToCmdLine("ch1", "msg"))
	intReply, ok := res.(*protocol.IntReply)
	if !ok || intReply.Code != 1 {
		t.Errorf("expected 1 subscriber, got %v", res)
	}
	if len(c2.Bytes()) == 0 {
		t.Error("expected message delivered to remaining subscriber")
	}
}

func TestUnsubscribeLastSubscriberRemovesChannel(t *testing.T) {
	hub := MakeHub()
	c := connection.NewFakeConn()
	_ = Subscribe(hub, c, utils.ToCmdLine("ch"))
	_ = UnSubscribe(hub, c, utils.ToCmdLine("ch"))
	if hub.subs.Len() != 0 {
		t.Errorf("expected empty hub, got %d channels", hub.subs.Len())
	}
	if c.SubsCount() != 0 {
		t.Errorf("expected no subscriptions, got %d", c.SubsCount())
	}
}

func TestUnsubscribeWithNoChannels(t *testing.T) {
	hub := MakeHub()
	c := connection.NewFakeConn()
	res := UnSubscribe(hub, c, utils.ToCmdLine())
	if res == nil {
		t.Fatal("expected nil reply for unsubscribe, got reply")
	}
	if string(c.Bytes()) != string(unSubscribeNothing) {
		t.Errorf("expected unsubscribe-with-no-channel reply, got %s", string(c.Bytes()))
	}
}

func TestUnsubscribeAll(t *testing.T) {
	hub := MakeHub()
	c := connection.NewFakeConn()
	_ = Subscribe(hub, c, utils.ToCmdLine("ch1", "ch2"))

	UnsubscribeAll(hub, c)
	if c.SubsCount() != 0 {
		t.Errorf("expected no subscriptions, got %d", c.SubsCount())
	}
	if hub.subs.Len() != 0 {
		t.Errorf("expected empty hub, got %d channels", hub.subs.Len())
	}

	// unsubscribe from a connection with no subscriptions: no-op
	UnsubscribeAll(hub, c)
}

func TestPublishWrongArgCount(t *testing.T) {
	hub := MakeHub()
	res := Publish(hub, utils.ToCmdLine("only-channel"))
	argNumErr, ok := res.(*protocol.ArgNumErrReply)
	if !ok {
		t.Errorf("expected arg number error, got %v", res)
	}
	if ok && argNumErr.Cmd != "publish" {
		t.Errorf("expected publish command name, got %s", argNumErr.Cmd)
	}
}

func TestSubscribeMultipleChannels(t *testing.T) {
	hub := MakeHub()
	c := connection.NewFakeConn()
	res := Subscribe(hub, c, utils.ToCmdLine("a", "b", "c"))
	if res == nil {
		t.Fatal("expected nil reply for subscribe, got reply")
	}
	if c.SubsCount() != 3 {
		t.Errorf("expected 3 subscriptions, got %d", c.SubsCount())
	}
	got := string(c.Bytes())
	for _, ch := range []string{"a", "b", "c"} {
		if !strings.Contains(got, "$1\r\n"+ch+"\r\n") {
			t.Errorf("missing confirmation for channel %s, got %q", ch, got)
		}
	}
}
