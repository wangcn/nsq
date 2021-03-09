package defer_queue

import (
	"fmt"
	"log"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}


func TestDeliveryIndex_Add(t *testing.T) {
	resetDir()
	index, err := NewDeliveryIndex("demo", "__deferQ")
	index.Start()
	assert.NoError(t, err)
	var msgID MessageID
	for i := 1; i <= 10; i++ {
		copy(msgID[:],fmt.Sprint(i))
		_ = index.Add(msgID)
	}
	err = index.Close()
	assert.NoError(t, err)
	assert.Equal(t, 2, countDirFiles())
}

func TestDeliveryIndex_load(t *testing.T) {
	resetDir()
	index, err := NewDeliveryIndex("demo", "__deferQ")
	assert.NoError(t, err)
	var msgID MessageID
	for i := 1; i <= 10; i++ {
		copy(msgID[:],fmt.Sprint(i))
		index.Add(msgID)
	}
	_ = index.Close()

	index2, err := NewDeliveryIndex("demo", "__deferQ")
	assert.NoError(t, err)
	copy(msgID[:], fmt.Sprint(1))
	assert.Equal(t, true, index2.Exists(msgID))
	copy(msgID[:], fmt.Sprint(3))
	assert.Equal(t, true, index2.Exists(msgID))
	copy(msgID[:], fmt.Sprint(11))
	assert.Equal(t, false, index2.Exists(msgID))

	_ = index2.Close()
}

func TestDeliveryIndex_Remove(t *testing.T) {
	resetDir()
	index, err := NewDeliveryIndex("demo", "__deferQ")
	assert.NoError(t, err)
	index.Start()
	var msgID MessageID
	for i := 1; i <= 10; i++ {
		copy(msgID[:], fmt.Sprint(i))
		index.Add(msgID)
	}
	time.Sleep(1100 * time.Millisecond)
	err = index.Remove()
	assert.NoError(t, err)
	assert.Equal(t, 0, countDirFiles())
}

func TestDeliveryIndex_Memory(t *testing.T) {
	resetDir()
	PrintMemUsage()
	index, err := NewDeliveryIndex("demo", "__deferQ")
	assert.NoError(t, err)
	index.Start()
	start := time.Now()
	var msgID MessageID
	for i := 1; i <= 1000000; i++ {
		copy(msgID[:], fmt.Sprint(i))
		_ = index.Add(msgID)
	}
	duration := time.Since(start)
	t.Log("duration", duration)
	err = index.Close()
	assert.NoError(t, err)
	assert.Equal(t, 2, countDirFiles())
	PrintMemUsage()
}

func TestDeliveryIndex_loopSync(t *testing.T) {
	tick := time.NewTicker(time.Second)
	go func() {
		for range tick.C {
			log.Println("tick")
		}
		log.Println("end")
	}()
	time.Sleep(3 * time.Second)
	tick.Stop()
	time.Sleep(2 * time.Second)
}
