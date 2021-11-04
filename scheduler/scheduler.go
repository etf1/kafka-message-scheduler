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
	livenessChan chan schedule.Schedule
}

func New(s store.Store, collector instrument.Collector) Scheduler {
	ts := timers.New()
	missedEvents := newMissedEvents()
	events := make(chan Event, eventsChanBuffer)
	livenessChan := make(chan schedule.Schedule, 1)

	return Scheduler{
		collector,
		ts,
		s,
		events,
		missedEvents,
		livenessChan,
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
		log.Debugf("add to timers failed: %+v %v", s, errs)
		sch.events <- schedule.InvalidSchedule{
			Schedule: s,
			Errors:   errs,
		}
	} else {
		log.Debugf("added to timers: %+v", s)
	}
}

// processMissedEvent process old events
func (sch Scheduler) processMissedEvent(e Event) {
	switch evt := e.(type) {
	case schedule.MissedSchedule:
		sch.events <- evt
		sch.Inc(instrument.MissedSchedule)
	case schedule.Schedule:
		log.Debugf("from missed events: %+v", evt)
		sch.addToTimers(evt)
		sch.Inc(instrument.PlannedSchedule)
	default:
		sch.events <- evt
		sch.Inc(instrument.Other)
	}
}

// processMissedEvent process store events
func (sch Scheduler) processStoreEvent(since time.Time, e store.Event, coldEvents chan store.Event) {
	// if the event is old, send to the missed events for processing
	if time.Since(time.Unix(e.Timestamp(), 0)) > 1000*time.Millisecond {
		coldEvents <- e
		return
	}

	switch evt := e.(type) {
	case schedule.InvalidSchedule:
		sch.Before(instrument.InvalidSchedule, evt)
		sch.events <- evt
		sch.After(instrument.InvalidSchedule, evt, nil)
		sch.Inc(instrument.InvalidSchedule)
	case schedule.DeleteSchedules:
		sch.Timers.DeleteByFunc(evt.DeleteFunc)
		if sch.missedEvents.length() > 0 {
			coldEvents <- evt
		}
		sch.Inc(instrument.DeleteSchedules)
	case schedule.DeletedSchedule:
		sch.Before(instrument.DeletedSchedule, evt)
		sch.Timers.Delete(evt)
		if sch.missedEvents.contains(evt.Schedule) {
			coldEvents <- evt
		}
		sch.After(instrument.DeletedSchedule, evt, nil)
		sch.Inc(instrument.DeletedSchedule)
	// should be at the last position
	case schedule.Schedule:
		inRange := IsInRange(since, evt)
		if sch.Get(evt.ID()) != nil && !inRange {
			sch.Before(instrument.DeletedSchedule, evt)
			sch.Delete(evt)
			sch.After(instrument.DeletedSchedule, evt, nil)
		} else if inRange {
			log.Debugf("from store events: %+v", evt)
			sch.Before(instrument.PlannedSchedule, evt)
			sch.addToTimers(evt)
			sch.After(instrument.PlannedSchedule, evt, nil)
			// don't track isalive events
			if isIsAliveSchedule(evt) {
				break
			}
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
	// if liveness detected inform the dedicated channel only
	if isIsAliveSchedule(s) {
		sch.livenessChan <- s
		return
	}
	sch.events <- s
	sch.Inc(instrument.TriggeredSchedule)
}

func (sch Scheduler) IsAlive() bool {
	storeEvents := sch.Store.Events()

	epoch := 3 * time.Second
	sendTimeout := 1 * time.Second
	recvTimeout := 5 * time.Second

	// send isAlive witness probe as a "store" event and wait for it
	// in the liveness channel
	select {
	case storeEvents <- isAliveSchedule(epoch):
	case <-time.After(sendTimeout):
		// something wrong the channel is blocking
		log.Errorf("cannot send isalive probe in store events channel")
		return false
	}

	select {
	case <-sch.livenessChan:
		return true
	case <-time.After(recvTimeout):
		// something wrong islaive schedule was not triggered
		log.Errorf("timeout for isalive probe from liveness channel")
		return false
	}
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
		log.Printf("scheduler started, missed schedules since=%v", since)

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
					break
				}
				sch.processStoreEvent(since, e, coldEvents)
			}
		}
	}()
}
