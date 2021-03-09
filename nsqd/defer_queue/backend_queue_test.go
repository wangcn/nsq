package defer_queue

import (
	"fmt"
	"io"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func dqLogf(level LogLevel, f string, args ...interface{}) {
	log.Printf(f, args...)
}

func TestDiskQueue_Put(t *testing.T) {
	dq := NewBackend(nil, "1234", "__deferQ", 10000,
		10, 1000,
		-1, time.Second, dqLogf)
	msg := []byte("wangfei123")
	dq.Put(msg)
	dq.Close()
	return

}

func TestDiskQueue_ReadChan(t *testing.T) {
	dq := NewBackend(nil, "1234", "__deferQ", 10000,
		10, 1000,
		-1, time.Second, dqLogf)
	readChan := dq.ReadChan()
	t.Log("readChan", string(<-readChan))
	t.Log("readChan", string(<-readChan))
	t.Log("readChan", string(<-readChan))

	t.Log("try delete")
	dq.Empty()
	dq.Delete()
	t.Log("delete")
}

func TestDiskQueue_Scan(t *testing.T) {
	var err error
	dq := NewBackend(nil, "1234", "__deferQ", 10000,
		10, 1000,
		-1, time.Second, dqLogf)
	num := 10
	for i := 0; i < num; i++ {
		msg := []byte(fmt.Sprintf("wangfei123_%d", i))
		err = dq.Put(msg)
		assert.NoError(t, err)
	}
	for {
		_, err = dq.Scan()
		if err != nil {
			assert.ErrorIs(t, err, io.EOF)
			break
		}
	}
	_ = dq.Empty()
	_ = dq.Close()
}
