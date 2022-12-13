package scheduler_test

import (
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/etf1/kafka-message-scheduler/instrument"
	hmapcoll "github.com/etf1/kafka-message-scheduler/internal/collector/hmap"
	"github.com/etf1/kafka-message-scheduler/internal/store/hmap"
	"github.com/etf1/kafka-message-scheduler/schedule"
	"github.com/etf1/kafka-message-scheduler/schedule/simple"
	"github.com/etf1/kafka-message-scheduler/scheduler"
)

var (
	simpleSchedule = simple.NewSchedule
)

// ReceivedEvent holds the event and the real triggered time
type ReceivedEvent struct {
	scheduler.Event
	epoch int64
}

func (r ReceivedEvent) String() string {
	return fmt.Sprintf("{received=%v event=%v}", r.epoch, r.Event.String())
}

func printReceivedEvents(t *testing.T, arr []ReceivedEvent) {
	for i, evt := range arr {
		t.Logf("%v: %T %v", i, evt.Event, evt)
	}
}

// TODO: investiguate docker time.AfterFunc
// when running tests in docker, some events are triggered 1 second earlier for unknown reason.
// maybe docker or alpine issue
// So for now we are using this compare function to hide this side effect
func compareEpoch(t *testing.T, expected, got int64) {
	if math.Abs(float64(expected-got)) > 1 {
		t.Fatalf("unexpected epoch %v, wanted %v", got, expected)
	}
}

// in these tests there are 2 important things to know:
// 1- cold schedules: are schedules already presents in the store before the scheduler starts
// 2- live schedules: are schedules coming from the store after the scheduler started,
// they will be stored in memory that is why they are called "live"

// Rule #1: schedules should be triggered at the expected time
func TestScheduler_trigger_epoch(t *testing.T) {
	var schedules []schedule.Schedule
	store := hmap.New()
	coll := hmapcoll.New()

	now := time.Now()
	passed := now.Add(-20 * time.Second)
	cold := []schedule.Schedule{
		simpleSchedule("1", now.Add(2*time.Second), passed),
		simpleSchedule("2", now.Add(3*time.Second), passed),
	}
	schedules = append(schedules, cold...)

	// schedules added before scheduler starts
	for _, schedule := range cold {
		store.Add(schedule)
	}

	startOfToday := scheduler.StartOfToday()
	s := scheduler.New(store, coll, nil)
	defer s.Close()

	s.Start(startOfToday)

	// schedules added after scheduler started
	now = time.Now()
	live := []schedule.Schedule{
		simpleSchedule("3", now.Add(4*time.Second)),
		simpleSchedule("4", now.Add(5*time.Second)),
		simpleSchedule("5", now.Add(6*time.Second)),
		simpleSchedule("6", now.Add(7*time.Second)),
	}
	schedules = append(schedules, live...)

	for _, schedule := range live {
		store.Add(schedule)
	}

	events := s.Events()

	result := make([]ReceivedEvent, 0)
loop:
	for {
		select {
		case evt := <-events:
			epoch := time.Now().Unix()
			t.Logf("received %T %v\n", evt, evt)
			result = append(result, ReceivedEvent{
				evt,
				epoch,
			})
		case <-time.After(8 * time.Second):
			break loop
		}
	}

	printReceivedEvents(t, result)

	if len(result) != len(schedules) {
		t.Fatalf("unexpected result length: %v", len(result))
	}

	for _, evt := range result {
		sch, ok := evt.Event.(schedule.Schedule)
		if !ok {
			t.Fatalf("unexpected type: %T", evt)
		}

		compareEpoch(t, sch.Epoch(), evt.epoch)
	}
}

// Rule #2: when updating the epoch, schedules should be triggered at the new time
func TestScheduler_update_epoch(t *testing.T) {
	var schedules []schedule.Schedule
	store := hmap.New()
	coll := hmapcoll.New()

	now := time.Now()
	passed := now.Add(-20 * time.Second)
	cold := []schedule.Schedule{
		// upgrade to later
		simpleSchedule("1", now.Add(2*time.Second).Unix(), passed),
		simpleSchedule("1", now.Add(5*time.Second).Unix(), passed),
		// upgrade to earlier
		simpleSchedule("2", now.Add(4*time.Second).Unix(), passed),
		simpleSchedule("2", now.Add(3*time.Second).Unix(), passed),
	}
	schedules = append(schedules, cold...)

	// schedules added before scheduler starts
	for _, schedule := range cold {
		store.Add(schedule)
	}

	startOfToday := scheduler.StartOfToday()
	s := scheduler.New(store, coll, nil)
	defer s.Close()

	s.Start(startOfToday)

	// schedules added after scheduler started
	now = time.Now()
	live := []schedule.Schedule{
		// upgrade to later
		simpleSchedule("3", now.Add(3*time.Second).Unix()),
		simpleSchedule("3", now.Add(6*time.Second).Unix()),
		// upgrade to earlier
		simpleSchedule("4", now.Add(7*time.Second).Unix()),
		simpleSchedule("4", now.Add(5*time.Second).Unix()),
	}

	schedules = append(schedules, live...)
	for _, schedule := range live {
		store.Add(schedule)
	}

	events := s.Events()

	result := make([]ReceivedEvent, 0)
loop:
	for {
		select {
		case evt := <-events:
			epoch := time.Now().Unix()
			t.Logf("received %T %v\n", evt, evt)
			result = append(result, ReceivedEvent{
				evt,
				epoch,
			})
		case <-time.After(8 * time.Second):
			break loop
		}
	}

	printReceivedEvents(t, result)

	if len(result) != 4 {
		t.Fatalf("unexpected result length: %v", len(result))
	}

	getLatest := func(id string) schedule.Schedule {
		var s schedule.Schedule
		for i := range schedules {
			if schedules[i].ID() == id {
				s = schedules[i]
			}
		}
		return s
	}

	for _, evt := range result {
		s := evt.Event.(schedule.Schedule)
		latest := getLatest(s.ID())
		compareEpoch(t, latest.Epoch(), s.Epoch())
		// check we received the latest version of the schedule
		if latest.Timestamp() != s.Timestamp() {
			t.Fatalf("unexpected schedule event: %v", s)
		}
	}
}

// Rule #2: when receiving live schedules with an epoch > today: if the schedule already exists, it should be deleted
// else it should be ignored
func TestScheduler_live_schedules_not_today(t *testing.T) {
	store := hmap.New()
	coll := hmapcoll.New()

	startOfToday := scheduler.StartOfToday()
	s := scheduler.New(store, coll, nil)
	defer s.Close()

	s.Start(startOfToday)

	// schedules added after scheduler started
	now := time.Now()
	live := []schedule.Schedule{
		// shoud be ignored
		simpleSchedule("1", now.Add(26*time.Hour).Unix()),
		// should be added
		simpleSchedule("2", now.Add(1*time.Hour).Unix()),
	}

	for _, schedule := range live {
		store.Add(schedule)
	}

	events := s.Events()

	result := make([]ReceivedEvent, 0)
loop:
	for {
		select {
		case evt := <-events:
			epoch := time.Now().Unix()
			t.Logf("received %T %v\n", evt, evt)
			result = append(result, ReceivedEvent{
				evt,
				epoch,
			})
		case <-time.After(5 * time.Second):
			if len(s.GetPlannedSchedules()) != 1 {
				t.Fatalf("unexpected planned schedules length %v, should be 1", len(s.GetPlannedSchedules()))
			}
			// update schedule 2 epoch to + 26 hours, so should be deleted
			store.Add(simpleSchedule("2", now.Add(26*time.Hour).Unix()))
			time.Sleep(5 * time.Second)
			break loop
		}
	}

	printReceivedEvents(t, result)

	if len(result) != 0 {
		t.Fatalf("unexpected result length: %v", len(result))
	}
	if len(s.GetPlannedSchedules()) != 0 {
		t.Fatalf("unexpected planned schedules length: %v", len(s.GetPlannedSchedules()))
	}
}

// Rule #4: Update on live schedules with past epoch should be triggered as InvalidSchedule
func TestScheduler_outdated_schedule(t *testing.T) {
	store := hmap.New()
	coll := hmapcoll.New()

	now := time.Now()
	passed := now.Add(-20 * time.Second)

	lastEpoch := now.Add(-4 * time.Second).Unix()
	cold := []schedule.Schedule{
		simpleSchedule("1", now.Add(4*time.Second), passed),
		simpleSchedule("1", lastEpoch, passed),
	}
	// schedules added before scheduler start
	for _, schedule := range cold {
		store.Add(schedule)
	}

	startOfToday := scheduler.StartOfToday()
	s := scheduler.New(store, coll, nil)
	defer s.Close()

	s.Start(startOfToday)

	// schedules added after scheduler start
	now = time.Now()
	live := []schedule.Schedule{
		simpleSchedule("2", now.Add(3*time.Second)),
		simpleSchedule("2", lastEpoch),
		simpleSchedule("3", now.Add(-4*time.Second)),
	}
	for _, schedule := range live {
		store.Add(schedule)
	}

	events := s.Events()

	result := make([]ReceivedEvent, 0)
loop:
	for {
		select {
		case evt := <-events:
			epoch := time.Now().Unix()
			t.Logf("received %T %v\n", evt, evt)
			result = append(result, ReceivedEvent{
				evt,
				epoch,
			})
		case <-time.After(5 * time.Second):
			break loop
		}
	}

	if len(result) != 4 {
		t.Fatalf("unexpected result length: %v", len(result))
	}

	printReceivedEvents(t, result)

	for _, evt := range result {
		switch s := evt.Event.(type) {
		case schedule.InvalidSchedule:
			if s.ID() != "2" && s.ID() != "3" {
				t.Fatalf("unexpected invalid schedules: %v", s)
			}
		case schedule.MissedSchedule:
			if s.ID() != "1" && s.Epoch() != lastEpoch {
				t.Fatalf("unexpected missed schedules: %v", s)
			}
		case schedule.Schedule:
			if s.ID() != "2" || s.Epoch() != evt.epoch {
				t.Fatalf("unexpected epoch %v, wanted %v", evt.epoch, s.Epoch())
			}
		default:
			t.Fatalf("unexpected event type: %T", evt.Event)
		}
	}
}

// Rule #5: scheduler should not plan and trigger InvalidSchedule event
func TestScheduler_ignore_invalid_schedules(t *testing.T) {
	store := hmap.New()
	coll := hmapcoll.New()

	now := time.Now()
	passed := now.Add(-20 * time.Second)
	cold := []schedule.Schedule{
		simpleSchedule("", now.Add(4*time.Second), passed),
		simpleSchedule("1", -1, passed),
	}
	// schedules added before scheduler start
	for _, schedule := range cold {
		store.Add(schedule)
	}

	startOfToday := scheduler.StartOfToday()
	s := scheduler.New(store, coll, nil)
	defer s.Close()

	s.Start(startOfToday)

	// schedules added after scheduler start
	now = time.Now()
	live := []schedule.Schedule{
		simpleSchedule("", now.Add(3*time.Second)),
		simpleSchedule("2", -1),
	}
	for _, schedule := range live {
		store.Add(schedule)
	}

	events := s.Events()

	result := make([]ReceivedEvent, 0)
loop:
	for {
		select {
		case evt := <-events:
			epoch := time.Now().Unix()
			t.Logf("received %T %v\n", evt, evt)
			result = append(result, ReceivedEvent{
				evt,
				epoch,
			})
		case <-time.After(5 * time.Second):
			break loop
		}
	}

	if len(result) != 2 {
		t.Fatalf("unexpected result length: %v", len(result))
	}

	printReceivedEvents(t, result)

	invalidSchedules := make([]schedule.InvalidSchedule, 0)

	for _, evt := range result {
		switch s := evt.Event.(type) {
		case schedule.InvalidSchedule:
			invalidSchedules = append(invalidSchedules, s)
		default:
			t.Fatalf("unexpected event type: %T", evt.Event)
		}
	}

	if len(invalidSchedules) != 2 {
		t.Fatalf("unexpected invalid schedules length: %v", len(invalidSchedules))
	}
}

// Rule #6: If there is a mix of cold and live schedules, when scheduler starts MissedSchedule type should be triggered first
func TestScheduler_missed_schedules_first(t *testing.T) {
	var schedules []schedule.Schedule
	store := hmap.New()
	coll := hmapcoll.New()

	now := time.Now()
	passed := now.Add(-20 * time.Second)
	cold := []schedule.Schedule{
		simpleSchedule("1", now.Add(1*time.Second), passed),
		simpleSchedule("2", now.Add(-2*time.Second), passed),
		simpleSchedule("3", now.Add(2*time.Second), passed),
		simpleSchedule("4", now.Add(-3*time.Second), passed),
	}
	schedules = append(schedules, cold...)

	// schedules added before scheduler start
	for _, schedule := range cold {
		store.Add(schedule)
	}

	startOfToday := scheduler.StartOfToday()
	s := scheduler.New(store, coll, nil)
	defer s.Close()

	s.Start(startOfToday)

	// schedules added after scheduler start
	now = time.Now()
	live := []schedule.Schedule{
		simpleSchedule("5", now.Add(3*time.Second).Unix()),
	}
	schedules = append(schedules, live...)
	for _, schedule := range live {
		store.Add(schedule)
	}

	events := s.Events()

	result := make([]ReceivedEvent, 0)
loop:
	for {
		select {
		case evt := <-events:
			epoch := time.Now().Unix()
			t.Logf("received %T %v\n", evt, evt)
			result = append(result, ReceivedEvent{
				evt,
				epoch,
			})
		case <-time.After(5 * time.Second):
			break loop
		}
	}

	printReceivedEvents(t, result)

	if len(result) != len(schedules) {
		t.Fatalf("unexpected result length: %v", len(result))
	}

	missed := true
	for i, evt := range result {
		_, ok := evt.Event.(schedule.MissedSchedule)
		if !ok {
			if i == 0 {
				t.Fatalf("unexpected event found at index 0: %T", evt)
			}
			missed = false
		}
		if ok && missed == false {
			t.Fatalf("all missed schedules should be first")
		}
	}
}

// Rule #7: scheduler.DeletedSchedule should delete and therefore avoid the trigger of a planned schedule
func TestScheduler_deleted_schedule(t *testing.T) {
	store := hmap.New()
	coll := hmapcoll.New()

	now := time.Now()
	passed := now.Add(-20 * time.Second)

	// schedules added before scheduler start
	store.Add(simpleSchedule("1", now.Add(2*time.Second), passed))
	store.Delete(simpleSchedule("1", 0, passed))

	startOfToday := scheduler.StartOfToday()
	s := scheduler.New(store, coll, nil)
	defer s.Close()

	s.Start(startOfToday)

	// schedules added after scheduler start
	now = time.Now()
	store.Add(simpleSchedule("2", now.Add(3*time.Second)))
	store.Delete(simpleSchedule("2", 0))

	events := s.Events()

	result := make([]ReceivedEvent, 0)
loop:
	for {
		select {
		case evt := <-events:
			epoch := time.Now().Unix()
			t.Logf("received %T %v\n", evt, evt)
			result = append(result, ReceivedEvent{
				evt,
				epoch,
			})
		case <-time.After(10 * time.Second):
			break loop
		}
	}

	printReceivedEvents(t, result)

	if len(result) != 0 {
		t.Fatalf("unexpected result length: %v", len(result))
	}
}

// Rule #8: scheduler.DeleteSchedules should delete and therefore avoid the trigger of a planned schedules
func TestScheduler_delete_schedules(t *testing.T) {
	store := hmap.New()
	coll := hmapcoll.New()

	expected := make([]schedule.Schedule, 0)

	now := time.Now()
	passed := now.Add(-20 * time.Second)

	// schedules added before scheduler start
	s1 := simpleSchedule("1", now.Add(2*time.Second), passed)
	store.Add(s1)
	expected = append(expected, s1)

	startOfToday := scheduler.StartOfToday()
	s := scheduler.New(store, coll, nil)
	defer s.Close()

	s.Start(startOfToday)

	now = time.Now()
	s3 := simpleSchedule("3", now.Add(3*time.Second))
	expected = append(expected, s3)

	// schedules added after scheduler start
	store.Add(simpleSchedule("2", now.Add(4*time.Second)))
	store.DeleteByFunc(func(sch schedule.Schedule) bool {
		return sch.ID() == "2"
	})
	store.Add(s3)

	events := s.Events()

	result := make([]ReceivedEvent, 0)
loop:
	for {
		select {
		case evt := <-events:
			epoch := time.Now().Unix()
			t.Logf("received %T %v\n", evt, evt)
			result = append(result, ReceivedEvent{
				evt,
				epoch,
			})
		case <-time.After(5 * time.Second):
			break loop
		}
	}

	printReceivedEvents(t, result)

	schedules := make([]schedule.Schedule, 0)
	for _, evt := range result {
		schedules = append(schedules, evt.Event.(schedule.Schedule))
	}

	if len(result) != 2 {
		t.Fatalf("unexpected result length: %v", len(result))
	}

	if !schedule.AreSame(schedules, expected) {
		t.Fatalf("unexpected result: %v", schedules)
	}
}

// Rule #9: trigger of missed schedules depends on the since parameter
func TestScheduler_scheduler_since(t *testing.T) {
	cases := []struct {
		name                string
		since               time.Time
		expectedMissedCount int
	}{
		{"since today", scheduler.StartOfToday(), 0},
		{"since yesterday", scheduler.StartOfYesterday(), 1},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := hmap.New()
			coll := hmapcoll.New()

			now := time.Now()
			passed := now.Add(-20 * time.Second)
			cold := []schedule.Schedule{
				simpleSchedule("1", now.Add(-24*time.Hour).Unix(), passed),
				simpleSchedule("2", now.Add(2*time.Second).Unix(), passed),
				simpleSchedule("3", now.Add(24*time.Hour).Unix(), passed),
			}

			// schedules added before scheduler start
			for _, schedule := range cold {
				store.Add(schedule)
			}

			s := scheduler.New(store, coll, nil)
			defer s.Close()

			s.Start(c.since)

			events := s.Events()

			result := make([]ReceivedEvent, 0)
		loop:
			for {
				select {
				case evt := <-events:
					epoch := time.Now().Unix()
					t.Logf("received %T %v\n", evt, evt)
					result = append(result, ReceivedEvent{
						evt,
						epoch,
					})
				case <-time.After(5 * time.Second):
					break loop
				}
			}

			printReceivedEvents(t, result)

			missedCount := 0
			for _, evt := range result {
				if _, ok := evt.Event.(schedule.MissedSchedule); ok {
					missedCount++
				}
			}

			if missedCount != c.expectedMissedCount {
				t.Fatalf("unexpected missed count %v, should be %v", missedCount, c.expectedMissedCount)
			}
		})
	}
}

// Rule #10: scheduler events have to be recorded in the collector
func TestScheduler_collector(t *testing.T) {
	store := hmap.New()
	coll := hmapcoll.New()

	now := time.Now()
	passed := now.Add(-20 * time.Second)
	cold := []schedule.Schedule{
		simpleSchedule("1", now.Add(-5*time.Second), passed),
	}

	// schedules added before scheduler start
	for _, schedule := range cold {
		store.Add(schedule)
	}

	startOfToday := scheduler.StartOfToday()
	s := scheduler.New(store, coll, nil)
	defer s.Close()

	s.Start(startOfToday)

	// schedules added after scheduler start
	now = time.Now()
	live := []schedule.Schedule{
		simpleSchedule("2", -1),
		simpleSchedule("3", now.Add(3*time.Second).Unix()),
		simpleSchedule("4", now.Add(3*time.Second).Unix()),
	}

	for _, schedule := range live {
		store.Add(schedule)
	}

	store.Delete(simpleSchedule("3", 0))

	events := s.Events()

	result := make([]ReceivedEvent, 0)
loop:
	for {
		select {
		case evt := <-events:
			epoch := time.Now().Unix()
			t.Logf("received %T %v\n", evt, evt)
			result = append(result, ReceivedEvent{
				evt,
				epoch,
			})
		case <-time.After(5 * time.Second):
			break loop
		}
	}

	printReceivedEvents(t, result)

	t.Logf("metrics=%v", coll.GetAll())

	if coll.Get(instrument.DeletedSchedule) != 1 {
		t.Fatalf("unexpected collected count for deleted")
	}

	if coll.Get(instrument.InvalidSchedule) != 1 {
		t.Fatalf("unexpected collected count for invalid")
	}

	if coll.Get(instrument.MissedSchedule) != 1 {
		t.Fatalf("unexpected collected count for missed")
	}

	if coll.Get(instrument.PlannedSchedule) != 2 {
		t.Fatalf("unexpected collected count for planned")
	}

	if coll.Get(instrument.TriggeredSchedule) != 1 {
		t.Fatalf("unexpected collected count for triggered")
	}
}

// Rule #11: scheduler should be able to diagnose its internal functioning
func TestScheduler_isalive(t *testing.T) {
	store := hmap.New()
	coll := hmapcoll.New()

	startOfToday := scheduler.StartOfToday()
	s := scheduler.New(store, coll, nil)
	defer s.Close()

	s.Start(startOfToday)

	events := s.Events()

	result := make([]ReceivedEvent, 0)

	aliveChan := make(chan bool, 1)
	go func() {
		aliveChan <- s.IsAlive()
	}()

	isalive := false

loop:
	for {
		select {
		case evt := <-events:
			epoch := time.Now().Unix()
			t.Logf("received %T %v\n", evt, evt)
			result = append(result, ReceivedEvent{
				evt,
				epoch,
			})
		case isalive = <-aliveChan:
		case <-time.After(7 * time.Second):
			t.Logf("timeout")
			break loop
		}
	}

	// isalive probes should not be visible in the scheduler triggered events output channel
	if len(result) != 0 {
		t.Fatalf("unexpected event in the triggered channel")
	}

	// metrics should not be added for isalive probes
	if l := len(coll.GetAll()); l != 0 {
		t.Fatalf("metrics should be 0, got %v", l)
	}

	if !isalive {
		t.Fatalf("scheduler is not alive :(")
	}
}

// Test issue #8 : https://github.com/etf1/kafka-message-scheduler/issues/8
// scheduler deadlock when many schedules with same id and same epoch=now
func TestScheduler_issue8(t *testing.T) {
	store := hmap.New()
	coll := hmapcoll.New()

	startOfToday := scheduler.StartOfToday()
	s := scheduler.New(store, coll, nil)
	defer s.Close()

	s.Start(startOfToday)

	// wait for the goroutine to be scheduled
	time.Sleep(1 * time.Second)

	now := time.Now().Add(1 * time.Second)
	max := 100
	for i := 1; i <= max; i++ {
		store.Add(simpleSchedule("1", now))
	}

	events := s.Events()

	result := make([]ReceivedEvent, 0)
loop:
	for {
		select {
		case evt := <-events:
			epoch := time.Now().Unix()
			t.Logf("received %T %v\n", evt, evt)
			result = append(result, ReceivedEvent{
				evt,
				epoch,
			})
		case <-time.After(10 * time.Second):
			break loop
		}
	}

	printReceivedEvents(t, result)

	if len(result) == 0 {
		t.Fatalf("unexpected result length: %v", len(result))
	}
}

// Test issue 32: https://github.com/etf1/kafka-message-scheduler/issues/32
// When scheduler is starting with processing missed events and there is a DeleteByFunc event, the scheduler
// seems to be in a dead lock situation
func TestScheduler_issue32(t *testing.T) {
	store := hmap.New()
	coll := hmapcoll.New()

	now := time.Now()
	passed := now.Add(-20 * time.Second)

	// schedules added before scheduler start
	for i := 1; i <= 1000; i++ {
		store.Add(simpleSchedule(i, now.Add(-10*time.Second), passed))
	}

	// delete half of the schedules
	store.DeleteByFunc(func(sch schedule.Schedule) bool {
		i, err := strconv.Atoi(sch.ID())
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		return i%2 == 0
	})

	startOfToday := scheduler.StartOfToday()
	s := scheduler.New(store, coll, nil)
	defer s.Close()

	s.Start(startOfToday)

	events := s.Events()

	result := make([]ReceivedEvent, 0)
loop:
	for {
		select {
		case evt := <-events:
			epoch := time.Now().Unix()
			t.Logf("received %T %v\n", evt, evt)
			result = append(result, ReceivedEvent{
				evt,
				epoch,
			})
		case <-time.After(10 * time.Second):
			t.Logf("select timeout")
			break loop
		}
	}

	printReceivedEvents(t, result)

	if len(result) != 500 {
		t.Fatalf("unexpected result length: %v", len(result))
	}
}
