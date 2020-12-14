package clientlib

import (
	"fmt"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	Epoch       = "scheduler-epoch"
	TargetTopic = "scheduler-target-topic"
	TargetKey   = "scheduler-target-key"
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

	if msg.TopicPartition.Topic == nil || len(*msg.TopicPartition.Topic) == 0 {
		return nil, fmt.Errorf("invalid parameter: topic nil or empty")
	}

	if len(schedulerTopic) == 0 {
		return nil, fmt.Errorf("invalid parameter: scheduler's topic empty")
	}

	if len(scheduleID) == 0 {
		return nil, fmt.Errorf("invalid parameter: schedule ID empty")
	}

	if epoch < time.Now().Unix() {
		return nil, fmt.Errorf("invalid parameter: invalid epoch")
	}

	bytes := func(i string) []byte {
		return []byte(i)
	}

	headers := msg.Headers

	headers = append(
		headers, kafka.Header{
			Key:   Epoch,
			Value: bytes(strconv.FormatInt(epoch, 10)),
		},
	)
	headers = append(
		headers, kafka.Header{
			Key:   TargetTopic,
			Value: bytes(*msg.TopicPartition.Topic),
		},
	)
	headers = append(
		headers, kafka.Header{
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

// DeleteSchedule create a message for deleting a schedule. This message goal is to be produce by a kafka producer.
func DeleteSchedule(scheduleID string, schedulerTopic string) (*kafka.Message, error) {
	// Invalid parameters
	if len(schedulerTopic) == 0 || len(scheduleID) == 0 {
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
