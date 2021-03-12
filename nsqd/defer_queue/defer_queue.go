package defer_queue

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/nsqio/go-diskqueue"

	"github.com/nsqio/nsq/internal/lg"
)

var (
	Size100GB  = int64(100 * 1024 * 1024 * 1024)
	SizeMinMsg = int32(10)
	SizeMaxMsg = int32(100 * 1024 * 1024)
)

type downStream struct {
	name string
	diskqueue.Interface
}

type DeferQueueInterface interface {
	Start()
	Put(message *Message) error
	Close() error
	RegDownStream(topicName string, dq diskqueue.Interface)
	DeRegDownStream(topicName string)
}

type deferQueue struct {
	pool         *deferBackendPool
	dataPath     string
	subPath      string
	syncInterval time.Duration
	needSync     bool

	readChan          chan []byte
	writeChan         chan Message
	writeResponseChan chan error
	exitFlag          int32
	exitChan          chan int
	exitSyncChan      chan int

	sync.RWMutex

	downStreamPool map[string]diskqueue.Interface
	// downStreamRegChan     chan downStream
	downStreamDeRegChan   chan string
	downStreamDeliverChan chan Message
	downStreamLock        sync.RWMutex

	timeSeg     int64
	curStartTs  int64
	lastStartTs int64

	sentIndex     *deliveryIndex
	lastSentIndex *deliveryIndex

	tw *TimeWheel

	logf AppLogFunc
}

func NewDeferQueue(dataPath string, timeSeg int64, logger *Logger) DeferQueueInterface {
	dqLogf := func(level LogLevel, f string, args ...interface{}) {
		lg.Logf(logger.Logger, logger.BaseLevel, lg.LogLevel(level), f, args...)
	}
	q := &deferQueue{
		pool:         newDeferBackendPool(dataPath, dqLogf),
		subPath:      "__deferQ",
		dataPath:     dataPath,
		syncInterval: time.Second,

		readChan:          make(chan []byte),
		writeChan:         make(chan Message),
		writeResponseChan: make(chan error),
		exitFlag:          0,
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),

		downStreamPool: make(map[string]diskqueue.Interface),
		// downStreamRegChan:     make(chan downStream),
		downStreamDeRegChan:   make(chan string),
		downStreamDeliverChan: make(chan Message),

		timeSeg: timeSeg,

		logf: dqLogf,
	}
	q.initPath()
	return q
}

func (h *deferQueue) Start() {
	h.load()
	h.processHistory()
	go h.ioLoop()
}

func (h *deferQueue) initPath() {
	var err error
	fullPath := path.Join(h.dataPath, h.subPath)
	if _, err = os.Stat(fullPath); os.IsNotExist(err) {
		err = os.Mkdir(fullPath, 0755)
		if err != nil {
			panic(err)
		}
	}
}

func (h *deferQueue) load() {
	fileName := h.metaDataFileName()
	h.pool.Load(fileName)
	h.tw = NewTimeWheel(time.Second, h.timeSeg, h.logf)
	h.tw.RegCallback(h.twCallback)
	h.tw.Start()
}

func (h *deferQueue) ioLoop() {
	var count int64
	var dataRead []byte

	syncTicker := time.NewTicker(h.syncInterval)
	gcTicker := time.NewTicker(time.Second)
	for {
		if h.needSync {
			err := h.persistMetaData()
			if err != nil {
				h.logf(ERROR, "defer queue persist meta failed - %s", err)
			}
			count = 0
		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set r to d.readChan only when there is data to read
		case dataRead = <-h.readChan:
			h.dispatch(dataRead)
		case dataWrite := <-h.writeChan:
			count++
			h.writeResponseChan <- h.writeOne(&dataWrite)
		// case downStreamIns := <-h.downStreamRegChan:
		// 	h.downStreamPool[downStreamIns.name] = downStreamIns.Interface
		case topicName := <-h.downStreamDeRegChan:
			h.logf(INFO, "deleting defer down stream: %s", topicName)
			delete(h.downStreamPool, topicName)
		case msg := <-h.downStreamDeliverChan:
			h.sendToTopic(&msg)
		case <-syncTicker.C:
			h.selectBackend()
			if count == 0 {
				// avoid sync when there's no activity
				continue
			}
			h.needSync = true
		case <-gcTicker.C:
			h.gc()
		case <-h.exitChan:
			goto exit
		}
	}
exit:
	h.logf(INFO, "DEFER_QUEUE: closing ... ioLoop")
	syncTicker.Stop()
	h.exitSyncChan <- 1
}

func (h *deferQueue) dispatch(dataRead []byte) {
	var msg Message
	_, _ = msg.UnmarshalMsg(dataRead)
	deferred := msg.Timestamp + msg.Deferred - time.Now().UnixNano()
	// 单位为纳秒。不足一秒时，直接投递。
	if deferred > int64(time.Second) {
		err := h.tw.AddMessage(&msg)
		if err != nil {
			h.logf(ERROR, "time wheel insert failed - %s", err)
		}
	} else {
		h.sendToTopic(&msg)
	}
}

func (h *deferQueue) twCallback(msg *Message) {
	h.downStreamDeliverChan <- *msg
}

func (h *deferQueue) sendToTopic(msg *Message) {
	// check whether being sent
	if h.sentIndex.Exists(msg.ID) {
		return
	}
	var ok bool
	var dq diskqueue.Interface
	h.downStreamLock.RLock()
	dq, ok = h.downStreamPool[msg.Topic]
	h.downStreamLock.RUnlock()
	if ok {
		nsqMsg := NsqMessage{
			ID:        msg.ID,
			Body:      msg.Body,
			Timestamp: msg.Timestamp,
			Attempts:  0,
		}
		writeNsqMessageToBackend(&nsqMsg, dq)
		h.sentIndex.Add(msg.ID)
	}
}

func (h *deferQueue) gc() {
	var lastStartTs int64
	// gc last file
	if h.lastStartTs != 0 {
		lastStartTs = h.lastStartTs
		h.pool.Remove(h.lastStartTs)
		h.lastStartTs = 0
	}
	// gc deliverIndex
	if h.lastSentIndex != nil {
		h.lastSentIndex.Close()
		h.lastSentIndex.Remove()
		h.lastSentIndex = nil
	}
	if lastStartTs > 0 {
		h.logf(INFO, "gc backend queue %d", lastStartTs)
	}
}

func (h *deferQueue) selectBackend() {
	var nextBlock BackendInterface
	var curBlock BackendInterface
	var ok bool
	now := time.Now().Unix()
	nextStartTs := now - now%h.timeSeg
	h.logf(DEBUG, "try to selectBackend. curTs: %d, nextTs: %d", h.curStartTs, nextStartTs)
	if h.curStartTs < nextStartTs {
		h.lastSentIndex = h.sentIndex
		h.sentIndex = nil
		curBlock, ok = h.pool.Get(h.curStartTs)
		// 当前文件读完后，方可处理下一个
		if ok && !curBlock.HasFinishedRead() {
			return
		}
		nextBlock, ok = h.pool.Get(nextStartTs)
		if ok {
			h.logf(INFO, "selectBackend. curTs: %d, nextTs: %d", h.curStartTs, nextStartTs)
			h.lastStartTs = h.curStartTs
			h.curStartTs = nextStartTs
			h.sentIndex, _ = NewDeliveryIndex(fmt.Sprintf("%d", h.curStartTs), path.Join(h.dataPath, h.subPath), h.logf)
			h.sentIndex.Start()
			nextBlock.SetReadChan(h.readChan)
		}
	}
}

// processHistory 队列启动时，清理已超时的文件块。
func (h *deferQueue) processHistory() {
	h.logf(INFO, "defer queue starts processing history file blocks")
	var msg Message
	var err error
	var b []byte
	for _, ts := range h.pool.AllStartPoint() {
		minTs := h.calcCurStartTs()
		if ts >= minTs {
			continue
		}
		backend, _ := h.pool.Get(ts)
		h.logf(DEBUG, "processHistory %d", ts)
		h.sentIndex, _ = NewDeliveryIndex(fmt.Sprintf("%d", ts), path.Join(h.dataPath, h.subPath), h.logf)
		for {
			b, err = backend.Scan()
			if err != nil {
				if err != io.EOF {
					h.logf(ERROR, "process history %d failed", ts)
				}
				break
			}
			_, err = msg.UnmarshalMsg(b)
			if err != nil {
				h.logf(ERROR, "process history %d failed. decode msg error", ts)
				break
			}
			h.sendToTopic(&msg)

		}
		h.pool.Remove(ts)
		h.sentIndex.Remove()
		h.sentIndex = nil

	}
}

func (h *deferQueue) writeOne(msg *Message) error {
	var err error
	var msgByte []byte
	actTime := (msg.Timestamp + msg.Deferred) / int64(time.Second)
	startPoint := actTime - actTime%h.timeSeg
	// endPoint := startPoint + h.timeSeg - 1f
	msgByte, err = msg.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if dq, ok := h.pool.Get(startPoint); ok {
		err = dq.Put(msgByte)
	} else {
		dq = h.pool.Create(startPoint, h.logf)
		err = dq.Put(msgByte)
	}
	return err
}

func (h *deferQueue) persistMetaData() error {
	fileName := h.metaDataFileName()
	err := h.pool.Persist(fileName)
	if err != nil {
		return err
	}
	h.needSync = false
	return nil
}

func (h *deferQueue) metaDataFileName() string {
	return path.Join(h.dataPath, h.subPath, "defer_queue.meta.dat")
}

func (h *deferQueue) getDeferListName(deliverTs int64, deferred int64) string {
	actTime := deliverTs + deferred
	return fmt.Sprintf("%d", actTime-actTime%h.timeSeg)
}

func (h *deferQueue) Put(msg *Message) error {
	h.RLock()
	defer h.RUnlock()

	if h.exitFlag == 1 {
		return errors.New("exiting")
	}
	h.writeChan <- *msg
	return <-h.writeResponseChan
}

func (h *deferQueue) Close() error {
	h.Lock()
	defer h.Unlock()
	var err error

	h.exitFlag = 1

	h.logf(INFO, "DEFER_QUEUE is closing")

	h.readChan = nil
	err = h.pool.Close()
	if err != nil {
		h.logf(ERROR, "close defer backend pool failed. err is %s", err)
	}
	h.tw.Stop()
	if h.sentIndex != nil {
		h.sentIndex.Close()
	}
	close(h.exitChan)
	// ensure that ioLoop has exited
	<-h.exitSyncChan

	return nil
}

func (h *deferQueue) RegDownStream(topicName string, dq diskqueue.Interface) {
	// h.downStreamRegChan <- downStream{
	// 	name:      topicName,
	// 	Interface: dq,
	// }
	h.downStreamLock.Lock()
	h.downStreamPool[topicName] = dq
	h.downStreamLock.Unlock()
}

func (h *deferQueue) DeRegDownStream(topicName string) {
	h.downStreamDeRegChan <- topicName
}

func (h *deferQueue) calcCurStartTs() int64 {
	now := time.Now().Unix()
	return now - now%h.timeSeg
}
