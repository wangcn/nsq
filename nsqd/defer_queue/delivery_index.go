package defer_queue

import (
	"bufio"
	"fmt"
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

	logf AppLogFunc
}

func NewDeliveryIndex(name string, filePath string, logf AppLogFunc) (*deliveryIndex, error) {
	var err error
	logf(DEBUG, "NewDeliveryIndex %s %s", name, filePath)
	ins := &deliveryIndex{
		filePath: filePath,
		name:     name,
		index:    make(map[MessageID]struct{}),
		stopChan: make(chan int),
		logf:     logf,
	}
	err = ins.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		logf(ERROR, "BACKEND(%s) failed to retrieve deliveryIndex MetaData - %s", name, err)
	}
	err = ins.load()
	if err != nil && !os.IsNotExist(err){
		logf(ERROR, "BACKEND(%s) failed to load deliveryIndex - %s", name, err)
		return nil, err
	}
	return ins, nil
}

func (h *deliveryIndex) Start() {
	// 单独启动同步循环。处理历史消息文件块时，不需要loopSync
	err := h.prepareWrite()
	if err != nil {
		h.logf(ERROR, "BACKEND(%s) failed to prepare deliveryIndex - %s", h.name, err)
	}
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
	f.Sync()
	f.Close()

	// atomically rename
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
	item := make([]byte, MsgIDLength)
	reader := bufio.NewReader(readFile)
	for {
		if readPos >= h.writePos {
			break
		}
		_, err = reader.Read(item)
		if err != nil {
			h.logf(DEBUG, "load delivery index err - %s", err)
			break
		}
		copy(msgID[:], item)
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
	if h.writeBuffer == nil {
		return nil
	}

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
