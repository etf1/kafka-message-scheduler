package kafka

import (
	"fmt"
	"strconv"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/etf1/kafka-message-scheduler/schedule"
)

const (
	Epoch       = "scheduler-epoch"
	TargetTopic = "scheduler-target-topic"
	TargetKey   = "scheduler-target-key"
)

type Schedule struct {
	*confluent.Message
}

func (s Schedule) getHeaderValue(key string) string {
	for _, header := range s.Headers {
		if header.Key == key && len(header.Value) > 0 {
			return string(header.Value)
		}
	}
	return ""
}

func (s Schedule) TargetTopic() string {
	return s.getHeaderValue(TargetTopic)
}

func (s Schedule) TargetKey() string {
	return s.getHeaderValue(TargetKey)
}

func (s Schedule) ID() string {
	return string(s.Key)
}

func (s Schedule) IsDeleted() bool {
	return s.Value == nil
}

/*
func (s Schedule) IsOutdated() bool {
	if s.Epoch() < time.Now().Unix() {
		return true
	}
	return false
}
*/
func (s Schedule) HasErrors() []error {
	return schedule.CheckSchedule(s)
}

func (s Schedule) Timestamp() int64 {
	return s.Message.Timestamp.Unix()
}

func (s Schedule) Epoch() int64 {
	epoch := s.getHeaderValue(Epoch)
	if epoch != "" {
		n, err := strconv.ParseInt(epoch, 10, 64)
		if err != nil {
			return 0
		}
		return n
	}
	return 0
}

func (s Schedule) String() string {
	return fmt.Sprintf("{id:'%v' epoch:%v date:%v ts:%v}", s.ID(), s.Epoch(), time.Unix(s.Epoch(), 0), time.Unix(s.Timestamp(), 0))
}
