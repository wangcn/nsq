package defer_queue

import (
	"fmt"
	"testing"
	"time"

	"github.com/nsqio/go-diskqueue"
	"github.com/stretchr/testify/assert"
)

func Test_writeMsg(t *testing.T) {
	type args struct {
		b []byte
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test1",
			args: args{
				b: []byte("wangfei\n"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeMsg(tt.args.b)
		})
	}
}

func TestMsgWriteNRead(t *testing.T) {
	dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
		t.Logf(f, args)
	}
	topicName := "__dq"
	dataPath := "."
	maxBytesPerFile := int64(1 * 1024 * 1024)
	minValidMsgLength := int32(10)
	maxMsgSize := 1 * 1024 * 1024 * 1024
	syncEvery := int64(100)
	syncTimeout := time.Second
	dq := diskqueue.New(
		topicName,
		dataPath,
		maxBytesPerFile,
		minValidMsgLength,
		int32(maxMsgSize)+minValidMsgLength,
		syncEvery,
		syncTimeout,
		dqLogf,
	)
	msg := Message{
		ID:        [16]byte{1},
		Body:      []byte("test"),
		Timestamp: time.Now().Unix(),
		Topic:     "pay",
		Deferred:  30,
	}
	N := 2
	var msgID MessageID
	for i := 1; i <= N; i++ {
		copy(msgID[:], fmt.Sprint(i))
		msg.ID = msgID
		msgByte, _ := msg.MarshalMsg(nil)
		dq.Put(msgByte)
	}

	for i := 1; i <= N; i++ {
		msg2 := Message{}
		ch := dq.ReadChan()
		content := <-ch
		l, e := msg2.UnmarshalMsg(content)
		assert.NoError(t, e)
		assert.Equal(t, 0, len(l))
		assert.Equal(t, i, msg2.ID)
	}

}
