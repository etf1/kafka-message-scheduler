package hmap

import (
	"sync"

	"github.com/etf1/kafka-message-scheduler/instrument"
)

// Hmap is simple, implementation of the instrument.Collector interface
type Hmap struct {
	metrics map[string]int
	mutex   *sync.RWMutex
}

func New() Hmap {
	return Hmap{
		metrics: make(map[string]int),
		mutex:   &sync.RWMutex{},
	}
}

func (h Hmap) Inc(e instrument.EventType) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.metrics[string(e)]++
}

func (h Hmap) Get(e instrument.EventType) int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	return h.metrics[string(e)]
}

func (h Hmap) GetAll() map[string]int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	return h.metrics
}
