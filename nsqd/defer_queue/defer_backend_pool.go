package defer_queue

import (
	"bufio"
	"container/list"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type deferBackendPool struct {
	data     map[int64]BackendInterface
	linkList *list.List

	dataPath string
	subPath  string

	logf AppLogFunc

	sync.RWMutex
}

func newDeferBackendPool(dataPath string, logf AppLogFunc) *deferBackendPool {
	ins := &deferBackendPool{
		data:     make(map[int64]BackendInterface, 0),
		linkList: list.New(),

		dataPath: dataPath,
		subPath:  "__deferQ",

		logf: logf,
	}
	return ins
}

func (h *deferBackendPool) Load(fileName string) {
	var f *os.File
	var err error
	var line string

	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil && !os.IsNotExist(err) {
		panic(err)
	}
	if os.IsNotExist(err) {
		return
	}

	defer f.Close()

	r := bufio.NewReader(f)
	for {
		line, err = r.ReadString('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			panic(err)
		}
		name := strings.TrimSpace(line)
		startPoint, _ := strconv.ParseInt(name, 10, 64)
		h.Create(startPoint, h.logf)
	}
}

func (h *deferBackendPool) newDiskQueue(startTs int64, logf AppLogFunc) BackendInterface {
	name := strconv.FormatInt(startTs, 10)
	return NewBackend(
		nil,
		name,
		path.Join(h.dataPath, h.subPath),
		Size100GB,
		SizeMinMsg,
		SizeMaxMsg,
		-1,
		time.Second,
		logf,
	)
}

func (h *deferBackendPool) Create(startTs int64, logf AppLogFunc) BackendInterface {
	h.Lock()
	defer h.Unlock()
	if h.linkList.Len() == 0 {
		h.linkList.PushBack(startTs)
	} else if startTs < h.linkList.Front().Value.(int64) {
		h.linkList.PushFront(startTs)
	} else {
		for e := h.linkList.Front(); e != nil; e = e.Next() {
			ts := e.Value.(int64)
			if startTs > ts && (e.Next() == nil || startTs < e.Next().Value.(int64)) {
				h.linkList.InsertAfter(startTs, e)
				break
			}
		}
	}
	q := h.newDiskQueue(startTs, logf)
	h.logf(INFO, "create backend %d", startTs)
	h.data[startTs] = q
	return q
}

func (h *deferBackendPool) Get(startTs int64) (queue BackendInterface, ok bool) {
	h.RLock()
	queue, ok = h.data[startTs]
	h.RUnlock()
	return
}

func (h *deferBackendPool) Remove(startTs int64) {
	h.Lock()
	defer h.Unlock()
	if _, ok := h.data[startTs]; ok {
		h.data[startTs].Empty()
		h.data[startTs].Delete()
	}
	for e := h.linkList.Front(); e != nil; {
		ts := e.Value.(int64)
		next := e.Next()
		if startTs == ts {
			h.linkList.Remove(e)
			break
		}
		e = next
	}
	delete(h.data, startTs)
}

func (h *deferBackendPool) First() BackendInterface {
	e := h.linkList.Front()
	return h.data[e.Value.(int64)]
}

func (h *deferBackendPool) Persist(fName string) error {

	var f *os.File
	var err error

	tmpFileName := fmt.Sprintf("%s.%d.tmp", fName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	for e := h.linkList.Front(); e != nil; e = e.Next() {
		ts := e.Value.(int64)
		line := strconv.FormatInt(ts, 10)
		_, err = fmt.Fprintf(f, line+"\n")
		if err != nil {
			_ = f.Close()
			return err
		}
	}

	_ = f.Sync()
	_ = f.Close()

	// atomically rename
	err = os.Rename(tmpFileName, fName)
	if err != nil {
		return err
	}
	return nil

}

func (h *deferBackendPool) AllStartPoint() []int64 {
	h.RLock()
	tsList := make([]int64, 0, h.linkList.Len())
	for e := h.linkList.Front(); e != nil; e = e.Next() {
		ts := e.Value.(int64)
		tsList = append(tsList, ts)
	}
	h.RUnlock()
	return tsList
}

func (h *deferBackendPool) Close() error {
	var err error
	for _, q := range h.data {
		err1 := q.Close()
		if err1 != nil {
			err = err1
		}
	}
	return err
}
