package defer_queue

import (
	"container/list"
	"errors"
	"log"
	"sync"
	"time"
)

type TimeWheel struct {
	interval    time.Duration
	ticker      *time.Ticker
	slots       []*list.List
	curPos      int64
	slotsNum    int64
	addChannel  chan Message
	stopChannel chan bool

	callback func(msg *Message)

	sync.RWMutex
	exitFlag int32

	logf AppLogFunc
}

func NewTimeWheel(interval time.Duration, slotsNum int64, logf AppLogFunc) *TimeWheel {
	tw := &TimeWheel{
		interval:    interval,
		slots:       make([]*list.List, slotsNum),
		curPos:      0,
		slotsNum:    slotsNum,
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
			return
		}
	}
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
		e = next
	}
}

func (h *TimeWheel) Stop() {
	h.Lock()
	defer h.Unlock()
	h.exitFlag = 1
	log.Println("timeWheel stop 1")
	h.stopChannel <- true
	log.Println("timeWheel stop 2")
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
	pos := h.getPos(msg)
	h.slots[pos].PushBack(msg)
}

func (h *TimeWheel) getPos(msg *Message) int64 {
	gap := msg.Deferred / int64(time.Second)
	return (gap + h.curPos) % h.slotsNum
}
