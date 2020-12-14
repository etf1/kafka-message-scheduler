package schedule

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidScheduleID     = errors.New("invalid schedule ID")
	ErrInvalidScheduleEpoch  = errors.New("invalid schedule Epoch")
	ErrOutdatedScheduleEpoch = errors.New("outdated schedule Epoch")
)

type Schedule interface {
	// schedule ID (should be uniq per schedule type)
	ID() string
	// schedule due date in seconds (epoch)
	Epoch() int64
	// Timestamp returns the creation date of the schedule
	Timestamp() int64
	// String returns a string representation
	String() string
}

// MissedSchedule represents schedules that was missed by the scheduler
type MissedSchedule struct {
	Schedule
}

// InvalidSchedule represents an invalid schedules (bad epoch)
type InvalidSchedule struct {
	Schedule
	Errors []error
}

// DeletedSchedule is a deleted schedule in the store
type DeletedSchedule struct {
	Schedule
}

// DeleteSchedules informs that schedules should be deleted based on the DeleteFunc
type DeleteSchedules struct {
	DeleteFunc func(s Schedule) bool
	Time       time.Time
}

func (d DeleteSchedules) String() string {
	return fmt.Sprintf("delete schedules with func: %T", d.DeleteFunc)
}

func (d DeleteSchedules) Timestamp() int64 {
	return d.Time.Unix()
}
