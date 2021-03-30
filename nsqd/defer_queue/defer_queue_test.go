package defer_queue

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nsqio/go-diskqueue"
	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/assert"

	"github.com/nsqio/nsq/internal/lg"
)

func decodeMessage(b []byte) (*NsqMessage, error) {
	var msg NsqMessage

	if len(b) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	copy(msg.ID[:], b[10:10+MsgIDLength])
	msg.Body = b[10+MsgIDLength:]

	return &msg, nil
}

func resetDir() {
	_ = os.RemoveAll("__deferQ")
	_ = os.Mkdir("__deferQ", 0755)
}

func countDirFiles() int {
	dir := "__deferQ"
	files, _ := ioutil.ReadDir(dir)
	return len(files)
}

// fun

func TestNewDeferQueue(t *testing.T) {
	resetDir()
	logger := &Logger{
		Logger:    log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lmicroseconds),
		BaseLevel: lg.LogLevel(DEBUG),
	}
	NewDeferQueue(".", 10, logger)
}

func TestDeferQueue_persistMetaData(t *testing.T) {
	resetDir()
	// q := NewDeferQueue(".", 10)
	// q.pool.Create(123)
	// q.pool.Create(456)
	// err := q.persistMetaData()
	// assert.NoError(t, err)
	// assert.Equal(t, 1, countDirFiles())
	// metaFileName := "defer_queue.meta.dat"
	// files, _ := ioutil.ReadDir("__deferQ")
	// assert.Equal(t, metaFileName, files[0].Name())
}

func TestDeferQueue_writeOne(t *testing.T) {
	resetDir()
	logger := &Logger{
		Logger:    log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lmicroseconds),
		BaseLevel: lg.LogLevel(DEBUG),
	}
	var err error
	msg := Message{
		ID:        [16]byte{1},
		Body:      []byte("wangfei"),
		Timestamp: time.Now().UnixNano(),
		Topic:     "test",
		Deferred:  int64(5 * time.Second),
	}
	deferQ := NewDeferQueue(".", 10, logger)
	err = deferQ.Put(&msg)
	assert.NoError(t, err)
	time.Sleep(1300 * time.Millisecond)
	assert.Equal(t, 3, countDirFiles())
}

func TestDeferQueue_writeTen(t *testing.T) {
	resetDir()
	logger := &Logger{
		Logger:    log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lmicroseconds),
		BaseLevel: lg.LogLevel(DEBUG),
	}
	var err error
	msg := Message{
		ID:        [16]byte{1},
		Body:      []byte("wangfei"),
		Timestamp: time.Now().UnixNano(),
		Topic:     "test",
		Deferred:  int64(5 * time.Second),
	}
	deferQ := NewDeferQueue(".", 1800, logger)
	var msgID MessageID
	for i := 1; i <= 11; i++ {
		copy(msgID[:], fmt.Sprint(i))
		msg.ID = msgID
		msg.Timestamp = time.Now().UnixNano()
		err = deferQ.Put(&msg)
		assert.NoError(t, err)
		time.Sleep(time.Second)
	}
	time.Sleep(1300 * time.Millisecond)
	assert.Equal(t, 5, countDirFiles())
}

func TestDeferQueue_dispatch(t *testing.T) {
	resetDir()
	logger := &Logger{
		Logger:    log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lmicroseconds),
		BaseLevel: lg.LogLevel(DEBUG),
	}
	var err error
	dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
		log.Printf(f, args)
	}
	topicName := "demo"
	topicQ := diskqueue.New(topicName, "__deferQ", 1000000, 10, 10000, -1, time.Second, dqLogf)
	deferQ := NewDeferQueue(".", 1800, logger)
	deferQ.Start()
	deferQ.RegDownStream(topicName, downStream{
		name:      topicName,
		Interface: topicQ,
	})
	msg := Message{
		ID:        [16]byte{1},
		Body:      []byte(strings.Repeat("a", 100)),
		Timestamp: time.Now().UnixNano(),
		Topic:     topicName,
		Deferred:  int64(30000 * time.Second),
	}
	start := time.Now()
	for i := 0; i< 5000000;i++ {
		err = deferQ.Put(&msg)
	}
	PrintMemUsage()
	deferQ.Close()
	t.Log("duration", time.Since(start))
	assert.NoError(t, err)
	return

	time.Sleep(3 * time.Second)
	msg.ID = [16]byte{2}
	msg.Timestamp = time.Now().UnixNano()
	err = deferQ.Put(&msg)
	assert.NoError(t, err)

	time.Sleep(5 * time.Second)

	deferQ.Close()

	assert.Equal(t, 7, countDirFiles())

}

func TestDeferQueue_gc(t *testing.T) {
	resetDir()
	logger := &Logger{
		Logger:    log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lmicroseconds),
		BaseLevel: lg.LogLevel(DEBUG),
	}
	deferQ := NewDeferQueue(".", 10, logger)
	msg := Message{
		ID:        [16]byte{1},
		Body:      []byte("wangfei"),
		Timestamp: time.Now().UnixNano(),
		Topic:     "demo",
		Deferred:  int64(2 * time.Second),
	}
	_ = deferQ.Put(&msg)

	msg.Deferred = int64(12 * time.Second)
	_ = deferQ.Put(&msg)
	time.Sleep(time.Second * 2)
	assert.Equal(t, 5, countDirFiles())

	time.Sleep(13 * time.Second)

	assert.Equal(t, 3, countDirFiles())
}

func TestDeferQueue_readTopic(t *testing.T) {
	cfg := nsq.NewConfig()
	cfg.MaxAttempts = 5
	producer, err := nsq.NewProducer("127.0.0.1:4150", cfg)
	assert.NoError(t, err)
	for i := 1; i <= 3; i++ {
		err = producer.DeferredPublish("test", time.Duration(3)*time.Second, []byte(fmt.Sprint(i * 10000)))
		assert.NoError(t, err)
	}
	var count int64
	// consumer
	work := func(message *nsq.Message) (err error) {
		atomic.AddInt64(&count, 1)
		t.Log("nnnnnnnnnn", message.Body)
		return err
	}
	consumer, err := nsq.NewConsumer("test", "defer", cfg)
	consumer.AddHandler(nsq.HandlerFunc(work))
	err = consumer.ConnectToNSQD("127.0.0.1:4150")
	assert.NoError(t, err)
	tick := time.NewTicker(time.Second)
	for {
		select {
		case <-tick.C:
			fmt.Println("count is", atomic.LoadInt64(&count))
		}
	}
}

func TestDeferQueue_readTopic2(t *testing.T) {
	cfg := nsq.NewConfig()
	cfg.MaxAttempts = 5
	producer, err := nsq.NewProducer("127.0.0.1:4150", cfg)
	assert.NoError(t, err)
	start := time.Now()
	var sendCount int64
	for {
		err = producer.DeferredPublish("test", time.Duration(15)*time.Second, []byte(time.Now().Format("2006-01-02 15:04:05")))
		assert.NoError(t, err)
		sendCount++
		if time.Since(start) > 3*time.Second {
			break
		}
	}
	t.Log("sendCount", sendCount)

	var count int64
	// consumer
	work := func(message *nsq.Message) (err error) {
		atomic.AddInt64(&count, 1)
		return err
	}
	consumer, err := nsq.NewConsumer("test", "defer", cfg)
	assert.NoError(t, err)
	consumer.AddHandler(nsq.HandlerFunc(work))
	err = consumer.ConnectToNSQD("127.0.0.1:4150")
	assert.NoError(t, err)
	tick := time.NewTicker(5 * time.Second)
	for range tick.C {
		consumeCount := atomic.LoadInt64(&count)
		log.Println("count is", consumeCount, "send count is", sendCount)
		if consumeCount == sendCount {
			producer.Stop()
			consumer.Stop()
			break
		}
	}
}

func TestDeferQueue_processHistory(t *testing.T) {
	resetDir()
	logger := &Logger{
		Logger:    log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lmicroseconds),
		BaseLevel: lg.LogLevel(DEBUG),
	}

	msg := Message{
		ID:        [16]byte{1},
		Body:      []byte("wangfei"),
		Timestamp: time.Now().UnixNano() - int64(10*time.Second),
		Topic:     "test",
		Deferred:  int64(5 * time.Second),
	}

	name := fmt.Sprint(time.Now().Add(-10 * time.Second).Unix())
	diskQ := NewBackend(nil, name, "__deferQ", 10000,
		10, 1000,
		-1, time.Second, dqLogf)
	var msgID MessageID
	for i := 1; i <= 2; i++ {
		copy(msgID[:], fmt.Sprint(i))
		msg.ID = msgID
		b, _ := msg.MarshalMsg(nil)
		diskQ.Put(b)
	}
	diskQ.Close()
	ioutil.WriteFile("__deferQ/defer_queue.meta.dat", []byte(name+"\n"), 0600)

	deferQ := NewDeferQueue(".", 10, logger)
	_ = deferQ
	time.Sleep(1300 * time.Millisecond)
	assert.Equal(t, 3, countDirFiles())

	resetDir()

	msg = Message{
		ID:        [16]byte{1},
		Body:      []byte("wangfei"),
		Timestamp: time.Now().UnixNano() - int64(10*time.Second),
		Topic:     "test",
		Deferred:  int64(5 * time.Second),
	}
	name = fmt.Sprint(time.Now().Add(10 * time.Second).Unix())
	diskQ = NewBackend(nil, name, "__deferQ", 10000,
		10, 1000,
		-1, time.Second, dqLogf)
	for i := 1; i <= 2; i++ {
		copy(msgID[:], fmt.Sprint(i))
		msg.ID = msgID
		b, _ := msg.MarshalMsg(nil)
		diskQ.Put(b)
	}
	diskQ.Close()
	ioutil.WriteFile("__deferQ/defer_queue.meta.dat", []byte(name+"\n"), 0600)

	deferQ = NewDeferQueue(".", 10, logger)
	_ = deferQ
	time.Sleep(1300 * time.Millisecond)
	assert.Equal(t, 3, countDirFiles())
}

func TestDeferQueue_ManyIOLoop(t *testing.T) {
	cfg := nsq.NewConfig()
	cfg.MaxAttempts = 5
	producer, err := nsq.NewProducer("127.0.0.1:4150", cfg)
	assert.NoError(t, err)
	tChan := make(chan int)
	var wg sync.WaitGroup
	work := func() {
		defer wg.Done()
		for deferTime := range tChan {
			producer.DeferredPublish("test", time.Duration(deferTime)*time.Second, []byte(time.Now().Format("2006-01-02 15:04:05")))
		}
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go work()
	}

	total := 1048576
	fileNum := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}
	stat := make(map[int]int, len(fileNum))
	batch := 5

	for rd := 0; rd < batch; rd++ {
		for _, num := range fileNum {
			t.Log("batch", rd, "fileNum", num)
			start := time.Now()
			jMax := num
			iMax := total / num
			for i := 0; i < iMax; i++ {
				for j := 1; j <= jMax; j++ {
					deferTime := j*1800 + 1500
					tChan <- deferTime
				}
			}
			// t.Logf("num is %d, duration is %f", num, time.Since(start).Seconds())
			duration := time.Since(start).Seconds()
			// fmt.Println(num, "\t", duration, "\t", int(float64(total)/duration))
			stat[num] += int(float64(total) / duration)
		}
	}
	close(tChan)
	wg.Wait()
	for _, num := range fileNum {
		fmt.Println(num, "\t", stat[num]/batch)
	}
}

func TestDeferQueue_TopicIOLoop(t *testing.T) {
	cfg := nsq.NewConfig()
	cfg.MaxAttempts = 5
	producer, err := nsq.NewProducer("127.0.0.1:4150", cfg)
	assert.NoError(t, err)
	tChan := make(chan int)
	var wg sync.WaitGroup
	work := func() {
		defer wg.Done()
		for range tChan {
			producer.Publish("test", []byte(time.Now().Format("2006-01-02 15:04:05")))
		}
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go work()
	}

	total := 1048576
	batch := 5
	stat := make([]int, 0, batch)

	for rd := 0; rd < batch; rd++ {
		t.Log("batch", rd)
		start := time.Now()
		for i := 0; i < total; i++ {
			tChan <- 1
		}
		duration := time.Since(start).Seconds()
		// fmt.Println(num, "\t", duration, "\t", int(float64(total)/duration))
		num := int(float64(total) / duration)
		stat = append(stat, num)
	}
	close(tChan)
	wg.Wait()
	for _, num := range stat {
		fmt.Println(num)
	}
}

func TestDeferQueue_CoIOLoop(t *testing.T) {
	tChan := make(chan int)
	var wg sync.WaitGroup
	work := func() {
		defer wg.Done()

		cfg := nsq.NewConfig()
		cfg.MaxAttempts = 5
		producer, err := nsq.NewProducer("127.0.0.1:4150", cfg)
		assert.NoError(t, err)
		count := 0
		for deferTime := range tChan {
			count++
			if count%1 == 1 {
				producer.Publish("test", []byte(time.Now().Format("2006-01-02 15:04:05")))
			} else {
				producer.DeferredPublish("test", time.Duration(deferTime)*time.Second, []byte(time.Now().Format("2006-01-02 15:04:05")))
			}
		}
		producer.Stop()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go work()
	}

	total := 1048576
	batch := 5
	stat := make([]int, 0, batch)

	for rd := 0; rd < batch; rd++ {
		t.Log("batch", rd)
		start := time.Now()
		for i := 0; i < total; i++ {
			tChan <- 2000
		}
		duration := time.Since(start).Seconds()
		num := int(float64(total) / duration)
		stat = append(stat, num)
	}
	close(tChan)
	wg.Wait()
	for _, num := range stat {
		fmt.Println(num)
	}
}

func TestDeferQueue_Consume(t *testing.T) {
	tChan := make(chan int)
	var wg sync.WaitGroup
	stopChan := make(chan int)
	exitGenChan := make(chan int)

	var totalWriteCount int64
	var totalReadCount int64

	ip := "127.0.0.1:4150"

	msgBody := []byte(strings.Repeat("a", 100))
	productWork := func() {
		defer wg.Done()

		cfg := nsq.NewConfig()
		cfg.MaxAttempts = 5
		producer, err := nsq.NewProducer(ip, cfg)
		assert.NoError(t, err)
		count := 0
		for deferTime := range tChan {
			atomic.AddInt64(&totalWriteCount, 1)
			count++
			if count%1 == 1 {
				producer.Publish("test", msgBody)
			} else {
				producer.DeferredPublish("test", time.Duration(deferTime)*time.Second, msgBody)
			}
		}
		producer.Stop()
	}

	handler := func(msg *nsq.Message) error {
		atomic.AddInt64(&totalReadCount, 1)
		return nil
	}

	consumeWork := func() {
		cfg := nsq.NewConfig()
		cfg.MaxAttempts = 5
		cfg.MaxInFlight = 30
		consumer, err := nsq.NewConsumer("test", "defer", cfg)
		assert.NoError(t, err)
		consumer.AddConcurrentHandlers(nsq.HandlerFunc(handler), 8)
		consumer.ConnectToNSQD(ip)
		<-stopChan
		consumer.Stop()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go productWork()
	}
	go func() {
		for {
			select {
			case <-stopChan:
				goto exit
			case tChan <- 3:

			}
		}
	exit:
		exitGenChan <- 1
	}()

	time.Sleep(4 * time.Second)
	go consumeWork()
	atomic.StoreInt64(&totalWriteCount, 0)
	// atomic.StoreInt64(&totalReadCount, 0)
	timeout := time.NewTimer(10 * time.Second)
	<-timeout.C
	close(stopChan)
	<-exitGenChan
	close(tChan)
	wg.Wait()
	t.Log("writes/s is", totalWriteCount/10)
	t.Log("read/s is", totalReadCount/10)
}
