package hmap

import (
	"sync"

	"github.com/etf1/kafka-message-scheduler/instrument"
	"github.com/etf1/kafka-message-scheduler/schedule"
)

// Hmap is simple, implementation of the instrument.Collector interface
type Hmap struct {
	before  map[string]int
	after   map[string]int
	metrics map[string]int
	mutex   *sync.RWMutex
}

func New() Hmap {
	return Hmap{
		metrics: make(map[string]int),
		before:  make(map[string]int),
		after:   make(map[string]int),
		mutex:   &sync.RWMutex{},
	}
}

func (h Hmap) Before(event instrument.EventType, message schedule.Schedule) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.before[string(event)]++
}

func (h Hmap) After(event instrument.EventType, message schedule.Schedule, err error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.after[string(event)]++
}

func (h Hmap) Inc(e instrument.EventType) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.metrics[string(e)]++
}

func (h Hmap) GetMetric(e instrument.EventType) int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	return h.metrics[string(e)]
}

func (h Hmap) GetMetrics() map[string]int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	return h.metrics
}

func (h Hmap) GetBefore(event instrument.EventType) int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	return h.before[string(event)]
}

func (h Hmap) GetAfter(event instrument.EventType) int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	return h.after[string(event)]
}
