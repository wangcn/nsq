package defer_queue

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"
)

type deliveryIndex struct {
	filePath    string
	name        string
	writeFile   *os.File
	writeBuffer *bufio.Writer
	writePos    int64
	index       map[MessageID]struct{}

	syncTick *time.Ticker
	stopChan chan int

	sync.Mutex
	needSync bool
}

func NewDeliveryIndex(name string, filePath string) (*deliveryIndex, error) {
	log.Println("NewDeliveryIndex", name, filePath)
	ins := &deliveryIndex{
		filePath: filePath,
		name:     name,
		index:    make(map[MessageID]struct{}),
		stopChan: make(chan int),
	}
	ins.retrieveMetaData()
	ins.load()
	ins.prepareWrite()
	return ins, nil
}

func (h *deliveryIndex) Start() {
	// 单独启动同步循环。处理历史消息文件块时，不需要loopSync
	go h.loopSync()
}

func (h *deliveryIndex) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := h.getMetaFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = fmt.Fscanf(f, "%d", &h.writePos)
	if err != nil {
		return err
	}

	return nil
}

func (h *deliveryIndex) persistMetaData() error {
	var f *os.File
	var err error

	fileName := h.getMetaFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d", h.writePos)
	if err != nil {
		f.Close()
		return err
	}
	log.Println("fffffffffffffffffff start", tmpFileName, fileName)
	f.Sync()
	f.Close()

	// atomically rename
	log.Println("rrrrrrrrrrrrrrrrrrr", tmpFileName, fileName)
	return os.Rename(tmpFileName, fileName)
}

func (h *deliveryIndex) getFileName() string {
	return path.Join(h.filePath, fmt.Sprintf("%s.delivery_index.dat", h.name))
}

func (h *deliveryIndex) getMetaFileName() string {
	return path.Join(h.filePath, fmt.Sprintf("%s.delivery_index.meta.dat", h.name))
}

func (h *deliveryIndex) load() error {
	var readPos int64
	fullName := h.getFileName()
	readFile, err := os.OpenFile(fullName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	var msgID MessageID
	reader := bufio.NewReader(readFile)
	for {
		if readPos >= h.writePos {
			break
		}
		err = binary.Read(reader, binary.BigEndian, &msgID)
		if err != nil {
			log.Println("debug", "load delivery index err", err)
			break
		}
		h.index[msgID] = struct{}{}
		readPos += MsgIDLength
	}
	readFile.Close()
	return nil
}

func (h *deliveryIndex) prepareWrite() error {
	var err error
	// prepare writeFile
	h.writeFile, err = os.OpenFile(h.getFileName(), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	if h.writePos > 0 {
		_, err = h.writeFile.Seek(h.writePos, 0)
		if err != nil {
			h.writeFile.Close()
			h.writeFile = nil
			return err
		}
	}
	h.writeBuffer = bufio.NewWriter(h.writeFile)

	return nil
}

func (h *deliveryIndex) Add(idx MessageID) error {
	h.Lock()
	defer h.Unlock()
	var err error

	h.index[idx] = struct{}{}
	_, err = h.writeBuffer.Write(idx[:])
	if err != nil {
		return err
	}
	h.writePos += MsgIDLength
	h.needSync = true
	return nil
}

func (h *deliveryIndex) loopSync() {
	h.syncTick = time.NewTicker(time.Second)
	for {
		select {
		case <-h.syncTick.C:
			h.Lock()
			h.sync()
			h.Unlock()
		case <-h.stopChan:
			return
		}
	}
}

func (h *deliveryIndex) sync() {
	if !h.needSync {
		return
	}
	_ = h.writeBuffer.Flush()
	_ = h.writeFile.Sync()
	err := h.persistMetaData()
	if err != nil {

		return
	}
	h.needSync = false
}

func (h *deliveryIndex) Exists(idx MessageID) bool {
	_, ok := h.index[idx]
	return ok
}

func (h *deliveryIndex) Close() error {
	h.stopChan <- 1
	h.persistMetaData()
	h.writeBuffer.Flush()
	h.writeFile.Sync()
	return h.writeFile.Close()
}

func (h *deliveryIndex) Remove() error {
	if h.writeFile != nil {
		h.writeBuffer.Flush()
		h.writeFile.Sync()
		h.writeFile.Close()
	}
	os.Remove(h.getMetaFileName())
	return os.Remove(h.getFileName())
}
