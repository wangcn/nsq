package defer_queue

import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type TimeWheel struct {
	interval    time.Duration
	ticker      *time.Ticker
	slots       []*list.List
	curPos      int64
	slotsNum    int64
	mask        int64
	addChannel  chan Message
	stopChannel chan bool
	total       int64

	callback func(msg *Message)

	sync.RWMutex
	exitFlag int32

	logf AppLogFunc
}

func normalizeSlotsNum(slotsNum int64) int64 {
	var normalNum int64 = 1
	for {
		if normalNum >= slotsNum {
			break
		}
		normalNum <<= 1
	}
	return normalNum
}

func NewTimeWheel(interval time.Duration, slotsNum int64, logf AppLogFunc) *TimeWheel {
	slotsNum = normalizeSlotsNum(slotsNum)
	tw := &TimeWheel{
		interval:    interval,
		slots:       make([]*list.List, slotsNum),
		curPos:      0,
		slotsNum:    slotsNum,
		mask:        slotsNum - 1,
		addChannel:  make(chan Message),
		stopChannel: make(chan bool),
		logf:        logf,
	}
	tw.initSlots()
	return tw
}

func (h *TimeWheel) initSlots() {
	for i := int64(0); i < h.slotsNum; i++ {
		h.slots[i] = list.New()
	}
}

func (h *TimeWheel) Start() {
	h.ticker = time.NewTicker(h.interval)
	go h.start()
}

func (h *TimeWheel) start() {
	l := h.slots[h.curPos]
	h.scanAndDispatch(l)
	for {
		select {
		case <-h.ticker.C:
			h.tickHandler()
		case msg := <-h.addChannel:
			h.add(&msg)
		case <-h.stopChannel:
			goto exit
		}
	}
exit:
	h.logf(INFO, "time wheel exit")
}

func (h *TimeWheel) tickHandler() {
	if h.curPos == h.slotsNum-1 {
		h.curPos = 0
	} else {
		h.curPos++
	}
	l := h.slots[h.curPos]
	h.scanAndDispatch(l)
}

func (h *TimeWheel) scanAndDispatch(l *list.List) {
	for e := l.Front(); e != nil; {
		msg := e.Value.(*Message)

		h.callback(msg)

		next := e.Next()
		l.Remove(e)
		atomic.AddInt64(&h.total, -1)
		e = next
	}
}

func (h *TimeWheel) Stop() {
	h.Lock()
	defer h.Unlock()
	h.exitFlag = 1
	h.logf(DEBUG, "time wheel stop 1")
	h.stopChannel <- true
	h.logf(DEBUG, "time wheel stop 2")
}

func (h *TimeWheel) AddMessage(msg *Message) error {
	h.RLock()
	defer h.RUnlock()
	if h.exitFlag == 1 {
		return errors.New("time wheel exiting")
	}

	h.addChannel <- *msg
	return nil
}

func (h *TimeWheel) RegCallback(fn func(msg *Message)) {
	h.callback = fn
}

func (h *TimeWheel) add(msg *Message) {
	atomic.AddInt64(&h.total, 1)
	pos := h.getPos(msg)
	h.slots[pos].PushBack(msg)
}

func (h *TimeWheel) getPos(msg *Message) int64 {
	gap := msg.Deferred / int64(time.Second)
	return (gap + h.curPos) & h.mask
}

func (h *TimeWheel) Count() int64 {
	return atomic.LoadInt64(&h.total)
}