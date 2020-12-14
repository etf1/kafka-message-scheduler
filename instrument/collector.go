package instrument

// EventType used for metric name
type EventType string

const (
	InvalidSchedule   = EventType("kafka_scheduler_invalid_schedule")
	DeletedSchedule   = EventType("kafka_scheduler_deleted_schedule")
	PlannedSchedule   = EventType("kafka_scheduler_planned_schedule")
	MissedSchedule    = EventType("kafka_scheduler_missed_schedule")
	TriggeredSchedule = EventType("kafka_scheduler_triggered_schedule")
	Other             = EventType("kafka_scheduler_other")
)

// Collector interface used for collecting metrics for each event types
type Collector interface {
	Inc(e EventType)
}
