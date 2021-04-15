package scheduler

import (
	"time"

	"github.com/etf1/kafka-message-scheduler/schedule"
	"github.com/etf1/kafka-message-scheduler/schedule/simple"
)

func StartOfToday() time.Time {
	return StartOfDayAsTime(0)
}

func StartOfYesterday() time.Time {
	return StartOfDayAsTime(-1)
}

func StartOfDayAsTime(delta int) time.Time {
	day := time.Now().AddDate(0, 0, delta)
	start := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, day.Location())

	return start
}

func StartOfDay(delta int) int64 {
	return StartOfDayAsTime(delta).Unix()
}

func EndOfToday() int64 {
	now := time.Now()
	end := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 0, now.Location())

	return end.Unix()
}

func IsInRange(since time.Time, s schedule.Schedule) bool {
	// typically, in range of the current day
	return since.Unix() <= s.Epoch() && s.Epoch() <= EndOfToday()
}

const isAliveID string = "||is-alive||"

func isAliveSchedule(d time.Duration) schedule.Schedule {
	return simple.NewSchedule(isAliveID, time.Now().Add(d))
}

func isIsAliveSchedule(s schedule.Schedule) bool {
	return s.ID() == isAliveID
}
