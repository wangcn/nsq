package defer_queue

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewTimeWheel(t *testing.T) {
	tw := NewTimeWheel(time.Second, 60)
	t.Log(tw.curPos)
	tw.Start()
	t.Log(time.Now().Second(), tw.curPos)
	assert.Equal(t, time.Now().Second(), tw.curPos)
}

func TestTimeWheel_AddMessage(t *testing.T) {
	tw := NewTimeWheel(time.Second, 60)
	tw.RegCallback(func(msg *Message) {
		log.Println("msg defer", msg.Deferred / int64(time.Second))
	})
	tw.Start()
	t.Log(tw.curPos)

	log.Println("now is", time.Now().Format("2006-01-02 15:04:05"))
	now := time.Now().UnixNano()
	msg := Message{}
	msg.Timestamp = now - 4*int64(time.Second)
	msg.Deferred = int64(5 * time.Second)
	msg.Deferred = msg.Timestamp + msg.Deferred - now
	tw.AddMessage(&msg)

	msg.Deferred = int64(7 * time.Second)
	msg.Deferred = msg.Timestamp + msg.Deferred - now
	tw.AddMessage(&msg)

	time.Sleep(8 * time.Second)
}

type cbDemo struct {
	pool map[string]struct{}
	num  int

	rw sync.RWMutex
}

func NewCbDemo() *cbDemo {
	return &cbDemo{
		rw:   sync.RWMutex{},
		pool: make(map[string]struct{}),
		num:  0,
	}
}

func (h *cbDemo) Info(msg *Message) {
	h.rw.RLock()
	log.Println("pool is", h.pool)
	log.Println("num is", h.num)
	h.rw.RUnlock()
}

func (h *cbDemo) Add(a string) {
	h.rw.Lock()
	h.pool[a] = struct{}{}
	h.num++
	h.rw.Unlock()
}

func TestTimeWheel_RegCallback(t *testing.T) {
	cbIns := NewCbDemo()
	tw := NewTimeWheel(time.Second, 60)
	tw.RegCallback(cbIns.Info)
	tw.Start()
	t.Log(tw.curPos)

	cbIns.Add("a")

	now := time.Now().UnixNano()
	msg := Message{}
	msg.Timestamp = now
	msg.Deferred = int64(2 * time.Second)
	tw.AddMessage(&msg)

	cbIns.Add("b")

	msg.Deferred = int64(3 * time.Second)
	tw.AddMessage(&msg)

	time.Sleep(4 * time.Second)
}
