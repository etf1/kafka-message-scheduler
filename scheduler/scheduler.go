package scheduler

import (
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/etf1/kafka-message-scheduler/instrument"
	"github.com/etf1/kafka-message-scheduler/internal/timers"
	"github.com/etf1/kafka-message-scheduler/schedule"
	"github.com/etf1/kafka-message-scheduler/store"
)

const (
	eventsChanBuffer = 1000000
)

type Event interface {
	String() string
}

type Error interface {
	Event
}

type Scheduler struct {
	instrument.Collector
	timers.Timers
	store.Store
	events       chan Event
	missedEvents *missedEvents
}

func New(s store.Store, collector instrument.Collector) Scheduler {
	ts := timers.New()
	missedEvents := newMissedEvents()
	events := make(chan Event, eventsChanBuffer)

	return Scheduler{
		collector,
		ts,
		s,
		events,
		missedEvents,
	}
}

func (sch Scheduler) Events() chan Event {
	return sch.events
}

func (sch Scheduler) Close() {
	log.Println("scheduler closing ...")
	sch.Timers.Close()
}

func (sch Scheduler) GetPlannedSchedules() []schedule.Schedule {
	return sch.GetAll()
}

func (sch Scheduler) addToTimers(s schedule.Schedule) {
	errs := sch.Timers.Add(s)
	if len(errs) > 0 {
		sch.events <- schedule.InvalidSchedule{
			Schedule: s,
			Errors:   errs,
		}
	}
}

// processMissedEvent process old events
func (sch Scheduler) processMissedEvent(e Event) {
	switch evt := e.(type) {
	case schedule.MissedSchedule:
		sch.events <- evt
		sch.Inc(instrument.MissedSchedule)
	case schedule.Schedule:
		sch.addToTimers(evt)
		sch.Inc(instrument.PlannedSchedule)
	default:
		sch.events <- evt
		sch.Inc(instrument.Other)
	}
}

// processMissedEvent process store events
func (sch Scheduler) processStoreEvent(since time.Time, e store.Event, coldEvents chan store.Event) {
	if time.Since(time.Unix(e.Timestamp(), 0)) > 1000*time.Millisecond {
		coldEvents <- e
		return
	}
	switch evt := e.(type) {
	case schedule.InvalidSchedule:
		sch.events <- evt
		sch.Inc(instrument.InvalidSchedule)
	case schedule.DeleteSchedules:
		sch.Timers.DeleteByFunc(evt.DeleteFunc)
		if sch.missedEvents.length() > 0 {
			coldEvents <- evt
		}
	case schedule.DeletedSchedule:
		sch.Timers.Delete(evt)
		if sch.missedEvents.contains(evt.Schedule) {
			coldEvents <- evt
		}
		sch.Inc(instrument.DeletedSchedule)
	// should be at the last position
	case schedule.Schedule:
		inRange := IsInRange(since, evt)
		if sch.Get(evt.ID()) != nil && !inRange {
			sch.Delete(evt)
		} else if inRange {
			sch.addToTimers(evt)
			sch.Inc(instrument.PlannedSchedule)
		}
		if sch.missedEvents.contains(evt) {
			coldEvents <- evt
		}
	default:
		sch.events <- evt
		sch.Inc(instrument.Other)
	}
}

func (sch Scheduler) processTimerEvent(s schedule.Schedule) {
	sch.events <- s
	sch.Inc(instrument.TriggeredSchedule)
}

func (sch Scheduler) Start(since time.Time) {
	storeEvents := sch.Store.Events()
	timerEvents := sch.Timers.Events()

	coldEvents := make(chan store.Event, eventsChanBuffer)

	sch.missedEvents.since = since
	missedEvents := sch.missedEvents.start(coldEvents)

	go func() {
		defer log.Println("scheduler closed ...")
		defer close(sch.events)

		// process incoming events from timers, store, missed events
		log.Printf("scheduler started, missed schedules since=%v\n", since)

	loop:
		for {
			select {
			// from timers
			case s, open := <-timerEvents:
				if !open {
					close(coldEvents)
					timerEvents = nil
					break
				}
				sch.processTimerEvent(s)
			// from missed handler
			case e, open := <-missedEvents:
				if !open {
					break loop
				}
				sch.processMissedEvent(e)
			// from store
			case e, open := <-storeEvents:
				if !open {
					sch.Close()
					// xxx storeEvents = nil
					break
				}
				sch.processStoreEvent(since, e, coldEvents)
			}
		}
	}()
}
