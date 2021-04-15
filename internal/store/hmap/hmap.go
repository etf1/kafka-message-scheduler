package hmap

import (
	"time"

	"github.com/etf1/kafka-message-scheduler/schedule"
	"github.com/etf1/kafka-message-scheduler/store"
)

const (
	eventsChanBuffer = 10000
)

// Hmap is an implementation of the store.Store interface
type Hmap struct {
	events chan store.Event
}

func New() Hmap {
	return Hmap{
		events: make(chan store.Event, eventsChanBuffer),
	}
}

func (h Hmap) Close() {
	close(h.events)
}

func (h Hmap) Events() chan store.Event {
	return h.events
}

func (h Hmap) Delete(s schedule.Schedule) {
	h.events <- schedule.DeletedSchedule{
		Schedule: s,
	}
}

// DeleteByFunc triggers the special event schedule.DeleteSchedules
// for the specified schedule
func (h Hmap) DeleteByFunc(s schedule.Schedule) {
	h.events <- schedule.DeleteSchedules{
		Time: time.Now(),
		DeleteFunc: func(sch schedule.Schedule) bool {
			return sch.ID() == s.ID()
		},
	}
}

func (h Hmap) Add(s schedule.Schedule) {
	sch := s
	errs := schedule.CheckSchedule(s)
	if len(errs) > 0 {
		sch = schedule.InvalidSchedule{
			Schedule: s,
			Errors:   errs,
		}
	}
	h.events <- sch
}
