package instrument

import (
	"github.com/etf1/kafka-message-scheduler/schedule"
)

// EventType used for metric name
type EventType string

const (
	InvalidSchedule   = EventType("kafka_scheduler_invalid_schedule")
	DeletedSchedule   = EventType("kafka_scheduler_deleted_schedule")
	DeleteSchedules   = EventType("kafka_scheduler_delete_schedules")
	PlannedSchedule   = EventType("kafka_scheduler_planned_schedule")
	MissedSchedule    = EventType("kafka_scheduler_missed_schedule")
	TriggeredSchedule = EventType("kafka_scheduler_triggered_schedule")
	ProducedSchedule  = EventType("kafka_scheduler_produced_schedule")
	Other             = EventType("kafka_scheduler_other")
)

// Collector interface used for collecting metrics for each event types
type Collector interface {
	Before(event EventType, message schedule.Schedule)
	After(event EventType, message schedule.Schedule, err error)
	Inc(e EventType)
}
