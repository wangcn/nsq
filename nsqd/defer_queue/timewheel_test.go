package defer_queue

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewTimeWheel(t *testing.T) {
	tw := NewTimeWheel(time.Second, 60, nil)
	t.Log(tw.curPos)
	tw.Start()
	t.Log(time.Now().Second(), tw.curPos)
	assert.Equal(t, time.Now().Second(), tw.curPos)
}

func TestTimeWheel_AddMessage(t *testing.T) {
	tw := NewTimeWheel(time.Second, 60, nil)
	tw.RegCallback(func(msg *Message) {
		log.Println("msg defer", msg.Deferred/int64(time.Second))
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
	tw := NewTimeWheel(time.Second, 60, nil)
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

func TestTimeWheel_getPos(t *testing.T) {
	tw := NewTimeWheel(time.Second, 60, logf)
	assert.Equal(t, int64(64), tw.slotsNum)
	assert.Equal(t, int64(63), tw.mask)
	t.Log(tw.curPos)
	tw.Start()
	now := time.Now().UnixNano()
	msg := Message{}
	msg.Timestamp = now
	msg.Deferred = int64(2 * time.Second)
	assert.Equal(t, int64(2), tw.getPos(&msg))
	msg.Deferred = int64(63 * time.Second)
	assert.Equal(t, int64(63), tw.getPos(&msg))
	msg.Deferred = int64(64 * time.Second)
	assert.Equal(t, int64(0), tw.getPos(&msg))
	msg.Deferred = int64(65 * time.Second)
	assert.Equal(t, int64(1), tw.getPos(&msg))
}

func mod_1(n int64) int64 {
	return n % 2048
}

func mod_2(n int64) int64 {
	return n & 2047
}

func TestTimeWheel_mod(t *testing.T) {
	for i := 0; i < 10000; i++ {
		assert.Equal(t, mod_1(int64(i)), mod_2(int64(i)))
	}
}

func BenchmarkTimeWheel_Mod(b *testing.B) {
	b.StopTimer()
	// ts := time.Now().Unix()
	var ret int64
	timeSeg := int64(2048)
	normalTimeSeg := int64(1)
	for {
		if normalTimeSeg >= timeSeg {
			break
		}
		normalTimeSeg += 1
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		ret = 1 % normalTimeSeg
	}
	b.StopTimer()
	modRet = ret
}

func BenchmarkTimeWheel_Mod2(b *testing.B) {
	b.StopTimer()
	ts := time.Now().Unix()
	var ret int64
	timeSeg := 1800
	normalTimeSeg := 1
	for {
		if normalTimeSeg >= timeSeg {
			break
		}
		normalTimeSeg <<= 1
	}
	normalTimeSeg--
	// b.Log("normalTimeSeg is", normalTimeSeg)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		ret = ts & int64(normalTimeSeg)
	}
	b.StopTimer()
	b.Log(ret)
}

var modRet int64

func BenchmarkTimeWheel_Mod3(b *testing.B) {
	b.StopTimer()
	// ts := time.Now().Unix()
	var ret int64
	// b.Log("normalTimeSeg is", normalTimeSeg)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		ret = mod_1(int64(i))
	}
	b.StopTimer()
	modRet = ret
}
