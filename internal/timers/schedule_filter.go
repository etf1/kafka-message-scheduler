package timers

// scheduleFilter is used to filter triggered timer events
// Why we need this ? because of DeleteByFunc. DeleteByFunc will deletes
// all timers in the timers map, but a timer can be fired just at the moment
// of the call to DeleteByFunc, so we needed to buffer all triggered timers and
// filter them by DeleteByFuncs, otherwise some event can be triggered multiple time

import (
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/etf1/kafka-message-scheduler/schedule"
)

// tells if a schedule should be deleted
type deletableScheduleFunc func(s schedule.Schedule) bool

// filters schedule based on the array deletesByFunc
type scheduleFilter struct {
	mutex         *sync.RWMutex
	deletesByFunc []deletableScheduleFunc
	input         chan schedule.Schedule
	output        chan schedule.Schedule
}

func newScheduleFilter(input chan schedule.Schedule) *scheduleFilter {
	return &scheduleFilter{
		mutex:         &sync.RWMutex{},
		deletesByFunc: make([]deletableScheduleFunc, 0),
		input:         input,
		output:        make(chan schedule.Schedule, eventsChanBuffer),
	}
}

func (sf scheduleFilter) start() {
	log.Printf("starting scheduler filter ...")

	go func() {
		defer log.Println("scheduler filter closed")
		defer close(sf.output)

		var s schedule.Schedule
		var filter, open bool
		for {
			s, open = <-sf.input
			if !open {
				break
			}

			filter = false
			sf.mutex.RLock()
			for i := range sf.deletesByFunc {
				if sf.deletesByFunc[i](s) {
					filter = true
					break
				}
			}
			sf.mutex.RUnlock()
			if !filter {
				sf.output <- s
			}
		}
	}()
}

// filterSchedule adds a DeleteByFunc function to the scheduleFilter slice
// it returns a done function to be called to remove the deleteByFunc from the slice
func (sf *scheduleFilter) filterSchedule(f deletableScheduleFunc) (done func()) {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()

	foundIndex := -1
	// search for a nil spot in the array
	for i := range sf.deletesByFunc {
		if sf.deletesByFunc[i] == nil {
			foundIndex = i
		}
	}

	// append to the end or set at found index
	if foundIndex == -1 {
		sf.deletesByFunc = append(sf.deletesByFunc, f)
		foundIndex = len(sf.deletesByFunc) - 1
	} else {
		sf.deletesByFunc[foundIndex] = f
	}

	return func() {
		sf.mutex.Lock()
		defer sf.mutex.Unlock()
		sf.deletesByFunc[foundIndex] = nil
	}
}
