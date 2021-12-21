package kafka

import (
	"encoding/json"
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
	for i := 0; i < len(s.Headers); i++ {
		if s.Headers[i].Key == key && len(s.Headers[i].Value) > 0 {
			return string(s.Headers[i].Value)
		}
	}
	return ""
}

func (s Schedule) TargetTopic() string {
	return s.getHeaderValue(TargetTopic)
}

func (s Schedule) Topic() string {
	return *s.TopicPartition.Topic
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

func (s Schedule) HasErrors() []error {
	return schedule.CheckSchedule(s)
}

func (s Schedule) Timestamp() int64 {
	return s.Message.Timestamp.Unix()
}

func (s Schedule) Epoch() int64 {
	base := 10
	bitSize := 64

	epoch := s.getHeaderValue(Epoch)
	if epoch != "" {
		n, err := strconv.ParseInt(epoch, base, bitSize)
		if err != nil {
			return 0
		}
		return n
	}
	return 0
}

func (s Schedule) Unwrap() interface{} {
	return s.Message
}

func (s Schedule) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})
	m["id"] = s.ID()
	m["epoch"] = s.Epoch()
	m["timestamp"] = s.Timestamp()
	m["topic"] = s.Topic()
	m["target-topic"] = s.TargetTopic()
	m["target-key"] = s.TargetKey()
	m["value"] = s.Value

	return json.Marshal(m)
}

func (s Schedule) String() string {
	return fmt.Sprintf("{id:'%v' epoch:%v date:%v ts:%v}", s.ID(), s.Epoch(), time.Unix(s.Epoch(), 0), time.Unix(s.Timestamp(), 0))
}
