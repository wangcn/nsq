package defer_queue

//go:generate msgp

import (
	"encoding/binary"
	"io"
	"os"
	"time"

	"github.com/nsqio/go-diskqueue"
)

const (
	MsgIDLength       = 16
	minValidMsgLength = 10
)

type MessageID [MsgIDLength]byte

type NsqMessage struct {
	ID        MessageID
	Body      []byte
	Timestamp int64
	Attempts  uint16
}

func (m *NsqMessage) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], m.Attempts)

	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

func writeNsqMessageToBackend(msg *NsqMessage, bq diskqueue.Interface) error {
	buf := bufferPoolGet()
	defer bufferPoolPut(buf)
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}
	return bq.Put(buf.Bytes())
}

type Message struct {
	ID        MessageID `msg:"1"`
	Body      []byte    `msg:"2"`
	Timestamp int64     `msg:"3"` // nanosecond
	Topic     string    `msg:"4"`
	Deferred  int64     `msg:"5"` // nanosecond
}

type deferWriter struct {
	name string
	w    io.Writer
}

var (
	writePool = make(map[int64]*deferWriter, 1440)
)

func getTimeSeg() int64 {
	ts := time.Now().Unix()
	return ts - ts%5
}

func writeMsg(b []byte) {
	timeSeg := getTimeSeg()
	var dw *deferWriter
	var ok bool
	if dw, ok = writePool[timeSeg]; !ok {
		fname := "defer_" + time.Unix(timeSeg, 0).Format("20060102150405")
		fh, _ := os.OpenFile(fname, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
		// w := bufio.NewWriter(fh)
		dw = &deferWriter{
			name: fname,
			w:    fh,
		}
		writePool[timeSeg] = dw
	}
	dw.w.Write(b)

}
