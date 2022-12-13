package scheduler

import (
	"errors"
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

// OutdatedScheduleStrategy allows to specify a strategy for outdated schedules
// either consider it as valid or invalid depending on a grace interval
type OutdatedScheduleStrategy interface {
	IsValid(s schedule.Schedule) bool
}

// check outdated schedule with a number of second between now and now - nbSecond
type intervalOutdatedScheduleStrategy uint

func (i intervalOutdatedScheduleStrategy) IsValid(s schedule.Schedule) bool {
	now := time.Now()
	if now.Add(time.Duration(-i)*time.Second).Unix() <= s.Epoch() && s.Epoch() <= now.Unix() {
		return true
	}
	return false
}

func NewOutdatedScheduleStrategyBySecond(nbSecond uint) OutdatedScheduleStrategy {
	return intervalOutdatedScheduleStrategy(nbSecond)
}

func DefaultOutdatedScheduleStrategyBySecond() OutdatedScheduleStrategy {
	return intervalOutdatedScheduleStrategy(0)
}

type Scheduler struct {
	instrument.Collector
	timers.Timers
	store.Store
	outdatedScheduleStrategy OutdatedScheduleStrategy
	events                   chan Event
	missedEvents             *missedEvents
	livenessChan             chan schedule.Schedule
}

func New(s store.Store, collector instrument.Collector, oss OutdatedScheduleStrategy) Scheduler {
	ts := timers.New()
	missedEvents := newMissedEvents()
	events := make(chan Event, eventsChanBuffer)
	livenessChan := make(chan schedule.Schedule, 1)

	sch := Scheduler{
		collector,
		ts,
		s,
		oss,
		events,
		missedEvents,
		livenessChan,
	}

	if oss == nil {
		sch.outdatedScheduleStrategy = DefaultOutdatedScheduleStrategyBySecond()
	}

	return sch
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
	err := sch.Timers.Add(s)
	if err != nil {
		log.Debugf("add to timers failed: %+v %v", s, err)
		// if err is outdated schedule
		if errors.Is(err, schedule.ErrOutdatedScheduleEpoch) {
			// check with defined outdated strategy, if the schedule is still valid
			if sch.outdatedScheduleStrategy.IsValid(s) {
				log.Debugf("outdated schedule strategy is valid: %+v", s)
				sch.events <- s
				sch.Inc(instrument.TriggeredSchedule)
			} else {
				log.Debugf("outdated schedule strategy is invalid: %+v", s)
				sch.events <- schedule.InvalidSchedule{
					Schedule: s,
					Error:    err,
				}
				sch.Inc(instrument.InvalidSchedule)
			}
		}
	} else {
		log.Debugf("schedule added to timers: %+v", s)
		// isAlive probe should not be added to collector
		if !isIsAliveSchedule(s) {
			sch.Inc(instrument.PlannedSchedule)
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
		log.Debugf("from missed events: %+v", evt)
		sch.addToTimers(evt)
	default:
		sch.events <- evt
		sch.Inc(instrument.Other)
	}
}

// processMissedEvent process store events
func (sch Scheduler) processStoreEvent(since time.Time, e store.Event, coldEvents chan store.Event) {
	if isOldEvent(e) {
		coldEvents <- e
		return
	}

	switch evt := e.(type) {
	case schedule.InvalidSchedule:
		sch.events <- evt
		sch.Inc(instrument.InvalidSchedule)
	case schedule.DeleteSchedules:
		sch.Timers.DeleteByFunc(evt.DeleteFunc)
		coldEvents <- evt
	case schedule.DeletedSchedule:
		sch.Timers.Delete(evt)
		coldEvents <- evt
		sch.Inc(instrument.DeletedSchedule)
	// should be at the last position
	case schedule.Schedule:
		inRange := IsInRange(since, evt)
		if sch.Get(evt.ID()) != nil && !inRange {
			sch.Delete(evt)
		} else if inRange {
			log.Debugf("from store events: %+v", evt)
			sch.addToTimers(evt)
			// don't track isalive events
			if isIsAliveSchedule(evt) {
				break
			}
		}
		coldEvents <- evt
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
