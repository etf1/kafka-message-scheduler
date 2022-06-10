package timers

// timers is holding all planned timers that will be triggered today
// it is a map of golang timers, triggered events are send to a channel
// and the scheduleFilter will filter this channel based on a list of delete functions

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/etf1/kafka-message-scheduler/schedule"
)

const (
	eventsChanBuffer = 1000000
)

type item struct {
	schedule.Schedule
	*time.Timer
}

type Timers struct {
	mutex          *sync.RWMutex
	items          map[string]item
	triggered      chan schedule.Schedule
	scheduleFilter *scheduleFilter
}

func New() Timers {
	timers := Timers{
		mutex:     &sync.RWMutex{},
		items:     make(map[string]item),
		triggered: make(chan schedule.Schedule, eventsChanBuffer),
	}

	scheduleFilter := newScheduleFilter(timers.triggered)
	scheduleFilter.start()

	timers.scheduleFilter = scheduleFilter

	return timers
}

func (ts Timers) Add(s schedule.Schedule) error {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	err := schedule.CheckSchedule(s)
	if err != nil {
		return err
	}

	if s.Epoch() < time.Now().Unix() {
		return fmt.Errorf("%w : %+v", schedule.ErrOutdatedScheduleEpoch, s)
	}

	// if found, we stop the existing timer
	if t, ok := ts.items[s.ID()]; ok {
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
	}

	// duration in second until the trigger time of the schedule
	epochDuration := time.Until(time.Unix(s.Epoch(), 0))
	ts.items[s.ID()] = item{
		s,
		time.AfterFunc(epochDuration, func() {
			ts.triggered <- s

			ts.mutex.Lock()
			defer ts.mutex.Unlock()

			delete(ts.items, s.ID())
		}),
	}

	return nil
}

// DeleteByFunc removes schedules in the timers map based on a function.
// if the function return true for the schedule, it will be deleted
func (ts Timers) DeleteByFunc(f func(s schedule.Schedule) bool) {
	defer ts.scheduleFilter.filterSchedule(f)()

	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	for _, t := range ts.items {
		if f(t.Schedule) {
			ts.deleteItem(t.Schedule.ID())
		}
	}
}

// Delete removes on schedule based on its id
func (ts Timers) Delete(s schedule.Schedule) {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	ts.deleteItem(s.ID())
}

func (ts Timers) deleteItem(id string) {
	if t, ok := ts.items[id]; ok {
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		delete(ts.items, id)
	}
}

func (ts Timers) Close() {
	defer log.Println("timers closed")

	log.Println("timers closing ...")
	ts.DeleteAll()
	close(ts.triggered)
}

func (ts Timers) DeleteAll() {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	for _, t := range ts.items {
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		delete(ts.items, t.ID())
	}
}

func (ts Timers) Get(id string) schedule.Schedule {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()

	if t, ok := ts.items[id]; ok {
		return t.Schedule
	}
	return nil
}

func (ts Timers) GetAll() []schedule.Schedule {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()

	result := make([]schedule.Schedule, 0)
	for _, v := range ts.items {
		result = append(result, v.Schedule)
	}
	return result
}

// Events returns the channel to listen for triggered timers
func (ts Timers) Events() chan schedule.Schedule {
	return ts.scheduleFilter.output
}
