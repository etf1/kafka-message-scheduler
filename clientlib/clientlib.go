package clientlib

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	Epoch       = "scheduler-epoch"
	TargetTopic = "scheduler-target-topic"
	TargetKey   = "scheduler-target-key"
	// SchedulerKey contains the header name of the header containing the original schedule id
	// This header is set when the message is produced by the scheduler in the target topic
	SchedulerKey = "scheduler-key"
)

// Schedule will create a schedule kafka message from a candidate message.
// It will not send the message to the scheduler's topic, just preparing the message to be produced.
// 'schedulerTopic' is the topic configured for a scheduler, 'scheduleID' is a unique ID used for the schedule
// for example: "video-online", 'epoch' represents the time when the message will be send to the original topic,
// it is epoch timestamp (since 1970), so epoch := plannedTime.Unix() where plannedTime is type of time.Time.
func Schedule(msg *kafka.Message, scheduleID string, epoch int64, schedulerTopic string) (*kafka.Message, error) {
	// Invalid parameters
	if msg == nil || len(msg.Value) == 0 {
		return nil, fmt.Errorf("invalid parameter: message nil or value is empty")
	}

	if msg.TopicPartition.Topic == nil || *msg.TopicPartition.Topic == "" {
		return nil, fmt.Errorf("invalid parameter: topic nil or empty")
	}

	if schedulerTopic == "" {
		return nil, fmt.Errorf("invalid parameter: scheduler's topic empty")
	}

	if scheduleID == "" {
		return nil, fmt.Errorf("invalid parameter: schedule ID empty")
	}

	if epoch < time.Now().Unix() {
		return nil, fmt.Errorf("invalid parameter: invalid epoch")
	}

	bytes := func(i string) []byte {
		return []byte(i)
	}

	headers := []kafka.Header{}

	// don't keep scheduler's specific header (with prefix "scheduler-")
	for _, h := range msg.Headers {
		if !strings.HasPrefix(h.Key, "scheduler-") {
			headers = append(headers, h)
		}
	}

	headers = append(
		headers, kafka.Header{
			Key:   Epoch,
			Value: bytes(strconv.FormatInt(epoch, 10)),
		},
		kafka.Header{
			Key:   TargetTopic,
			Value: bytes(*msg.TopicPartition.Topic),
		}, kafka.Header{
			Key:   TargetKey,
			Value: msg.Key,
		},
	)

	m := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &schedulerTopic, Partition: kafka.PartitionAny},
		Key:            bytes(scheduleID),
		Value:          msg.Value,
		Headers:        headers,
		Timestamp:      msg.Timestamp,
		TimestampType:  msg.TimestampType,
		Opaque:         msg.Opaque,
	}

	return &m, nil
}

// DeleteSchedule creates a message for deleting a schedule. This message goal is to be produce by a kafka producer.
func DeleteSchedule(scheduleID, schedulerTopic string) (*kafka.Message, error) {
	// Invalid parameters
	if schedulerTopic == "" || scheduleID == "" {
		return nil, fmt.Errorf("invalid parameter")
	}

	bytes := func(i string) []byte {
		return []byte(i)
	}

	m := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &schedulerTopic, Partition: kafka.PartitionAny},
		Key:            bytes(scheduleID),
		// tombstone message has nil Value
		Value: nil,
	}

	return &m, nil
}

// IsSchedulerMessage tells if a message is coming from the scheduler
func IsSchedulerMessage(msg *kafka.Message) bool {
	if msg == nil {
		return false
	}

	for _, h := range msg.Headers {
		if h.Key == SchedulerKey {
			return true
		}
	}

	return false
}
