package scheduler

// missedEvents is in charge of handling the cold schedules which are
// in the scheduler topic and has a timestamp older than current time.

import (
	log "github.com/sirupsen/logrus"

	"sort"
	"sync"
	"time"

	"github.com/etf1/kafka-message-scheduler/schedule"
	"github.com/etf1/kafka-message-scheduler/store"
)

type missedEvents struct {
	mutex     *sync.RWMutex
	schedules map[string]schedule.Schedule
	others    []store.Event
	since     time.Time
}

func newMissedEvents() *missedEvents {
	return &missedEvents{
		mutex:     &sync.RWMutex{},
		schedules: make(map[string]schedule.Schedule),
		others:    make([]store.Event, 0),
	}
}

func (m *missedEvents) reset() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// merges events by schedule ID
	m.schedules = make(map[string]schedule.Schedule)
	// contains everything else (i.e errors from the store, etc...)
	m.others = make([]store.Event, 0)
}

func (m *missedEvents) delete(s schedule.Schedule) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.schedules, s.ID())
}

func (m *missedEvents) deleteByFunc(f func(s schedule.Schedule) bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, s := range m.schedules {
		if f(s) {
			delete(m.schedules, s.ID())
		}
	}
}

func (m *missedEvents) add(s schedule.Schedule) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.schedules[s.ID()] = s
}

func (m *missedEvents) length() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.schedules) + len(m.others)
}

func (m *missedEvents) contains(s schedule.Schedule) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	_, found := m.schedules[s.ID()]
	return found
}

func (m *missedEvents) sendEvents(output chan Event) {
	sorted := make([]store.Event, 0)

	// flattern
	for _, sch := range m.schedules {
		sorted = append(sorted, sch)
	}

	// merge
	sorted = append(sorted, m.others...)

	// sort by timestamp
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Timestamp() < sorted[j].Timestamp()
	})

	// cast to scheduler.Event
	result := make([]Event, len(sorted))
	for i, evt := range sorted {
		result[i] = evt
	}

	// send
	for _, evt := range result {
		s, ok := evt.(schedule.Schedule)
		if ok {
			// this schedule was not triggered so send it as missed schedule event
			if s.Epoch() <= time.Now().Unix() {
				output <- schedule.MissedSchedule{
					Schedule: s,
				}
				continue
			}
		}

		output <- evt
	}
}

func isOldEvent(e store.Event) bool {
	return time.Since(time.Unix(e.Timestamp(), 0)) > 1000*time.Millisecond
}

func (m *missedEvents) processEvent(evt store.Event) {
	switch s := evt.(type) {
	case schedule.InvalidSchedule:
		// empty ID or epoch is invalid, skip
		// we don't want to fire this "legacy" invalids schedules
		// each time we resynch all events from the beginning
		break
	case schedule.DeletedSchedule:
		if m.length() > 0 {
			m.delete(s)
		}
	case schedule.DeleteSchedules:
		if m.length() > 0 {
			m.deleteByFunc(s.DeleteFunc)
		}
	case schedule.Schedule:
		inRange := IsInRange(m.since, s)
		if m.contains(s) && !inRange {
			m.delete(s)
		} else if inRange && isOldEvent(evt) {
			m.add(s)
		}
	default:
		m.others = append(m.others, evt)
	}
}

func (m *missedEvents) start(input <-chan store.Event) <-chan Event {
	output := make(chan Event, eventsChanBuffer)

	go func() {
		defer log.Printf("missed event handler closed")
		defer close(output)

		m.reset()

		// timeout, when no events are coming
		duration := 1000 * time.Millisecond
		timeout := time.NewTimer(duration)
		defer timeout.Stop()

	loop:
		for {
			// to avoid <-time.After(..) memory leak
			timeout.Reset(duration)

			select {
			// if no new messages, then break and process collected events
			case <-timeout.C:
				// process all collected events
				if len(m.schedules) != 0 || len(m.others) != 0 {
					m.sendEvents(output)
					m.reset()
				}
			case evt, open := <-input:
				if !open {
					// if remaining events
					m.sendEvents(output)
					break loop
				}
				m.processEvent(evt)
			}
		}
	}()

	return output
}
