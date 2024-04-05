package clientlib

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/etf1/kafka-message-scheduler/clientlib/retry"
)

const (
	// HeaderEpoch represents the epoch when the message have to be triggered.
	HeaderEpoch = "scheduler-epoch"

	// HeaderTargetTopic represents the target topic where the message will be triggered.
	HeaderTargetTopic = "scheduler-target-topic"

	// HeaderTargetKey represents the target key of the message that will be triggered.
	HeaderTargetKey = "scheduler-target-key"

	// HeaderRetryAttempt is the header key used in Kafka message to identify the retry attempt counter.
	HeaderRetryAttempt = "retry-attempt"

	// HeaderRetryOriginalTimestamp is the header key used in Kafka message to identify the original Kafka
	// message timestamp.
	HeaderRetryOriginalTimestamp = "retry-original-timestamp"

	// SchedulerKey contains the header name of the header containing the original schedule id
	// This header is set when the message is produced by the scheduler in the target topic
	SchedulerKey = "scheduler-key"
)

var (
	// ErrNoSchedulerTopic is throwed when no scheduler topic is provided.
	ErrNoSchedulerTopic = errors.New("invalid parameter: scheduler's topic empty")

	// ErrNoValue is throwed when a Kafka message does not have any value.
	ErrNoValue = errors.New("invalid parameter: invalid parameter: message nil or value is empty")

	// ErrNoTopicAssociated is throwed when a Kafka message does not have a topic associated.
	ErrNoTopicAssociated = errors.New("invalid parameter: topic nil or empty")

	// ErrNoSchedulerID is throwed when no scheduler identifier is specified.
	ErrNoSchedulerID = errors.New("invalid parameter: schedule ID empty")

	// ErrNoRetryConfigProvided is throwed when a no retry configuration is provided.
	ErrNoRetryConfigProvided = errors.New("no configuration retry provided, please specify one")

	// ErrInvalidEpoch is throwed when provided epoch is invalid.
	ErrInvalidEpoch = errors.New("invalid parameter: invalid epoch")

	// ErrMaxAttemptReached is throwed when a retried message has been attempted the maximum number of times.
	ErrMaxAttemptReached = errors.New("retry attempt reached max number of time")

	// Now allows to specify a custom current time in tests.
	Now = time.Now
)

// Schedule will create a schedule kafka message from a candidate message.
// It will not send the message to the scheduler's topic, just preparing the message to be produced.
// 'schedulerTopic' is the topic configured for a scheduler, 'scheduleID' is a unique ID used for the schedule
// for example: "video-online", 'epoch' represents the time when the message will be send to the original topic,
// it is epoch timestamp (since 1970), so epoch := plannedTime.Unix() where plannedTime is type of time.Time.
func Schedule(msg *kafka.Message, scheduleID string, epoch int64, schedulerTopic string) (*kafka.Message, error) {
	// Invalid parameters
	if msg == nil || len(msg.Value) == 0 {
		return nil, ErrNoValue
	}

	if msg.TopicPartition.Topic == nil || *msg.TopicPartition.Topic == "" {
		return nil, ErrNoTopicAssociated
	}

	if schedulerTopic == "" {
		return nil, ErrNoSchedulerTopic
	}

	if scheduleID == "" {
		return nil, ErrNoSchedulerID
	}

	if epoch < Now().Unix() {
		return nil, ErrInvalidEpoch
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
			Key:   HeaderEpoch,
			Value: []byte(strconv.FormatInt(epoch, 10)),
		},
		kafka.Header{
			Key:   HeaderTargetTopic,
			Value: []byte(*msg.TopicPartition.Topic),
		}, kafka.Header{
			Key:   HeaderTargetKey,
			Value: msg.Key,
		},
	)

	m := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &schedulerTopic, Partition: kafka.PartitionAny},
		Key:            []byte(scheduleID),
		Value:          msg.Value,
		Headers:        headers,
		Timestamp:      msg.Timestamp,
		TimestampType:  msg.TimestampType,
		Opaque:         msg.Opaque,
	}

	return &m, nil
}

// ScheduleRetry retries a Kafka message by using the specified retry configuration.
func ScheduleRetry(msg *kafka.Message, scheduleID, schedulerTopic string, config *retry.Config) (*kafka.Message, error) {
	if config == nil {
		return nil, ErrNoRetryConfigProvided
	}

	attempt := setHeaderRetryAttempt(msg)

	if attempt > config.MaxAttempt {
		return nil, ErrMaxAttemptReached
	} else if attempt == 1 {
		currentTimestamp := []byte(strconv.FormatInt(Now().Unix(), 10))
		updateOrAddHeader(msg, HeaderRetryOriginalTimestamp, currentTimestamp)
	}

	return Schedule(msg, scheduleID, getNextEpoch(attempt, config.Strategy), schedulerTopic)
}

// DeleteSchedule creates a message for deleting a schedule. This message goal is to be produce by a kafka producer.
func DeleteSchedule(scheduleID, schedulerTopic string) (*kafka.Message, error) {
	// Invalid parameters
	if schedulerTopic == "" || scheduleID == "" {
		return nil, fmt.Errorf("invalid parameter")
	}

	m := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &schedulerTopic, Partition: kafka.PartitionAny},
		Key:            []byte(scheduleID),
		// tombstone message has nil Value
		Value: nil,
	}

	return &m, nil
}

// IsSchedulerMessage tells if a message is coming from the scheduler.
func IsSchedulerMessage(msg *kafka.Message) bool {
	return getHeader(msg, SchedulerKey).Key == SchedulerKey
}

// IsRetriedMessage tells if a message has already been retried by the scheduler.
func IsRetriedMessage(msg *kafka.Message) bool {
	return getHeader(msg, HeaderRetryAttempt).Key == HeaderRetryAttempt
}

func setHeaderRetryAttempt(msg *kafka.Message) int {
	attempt := 1
	if value, hasValue := getHeaderValue(msg, HeaderRetryAttempt); hasValue {
		if attemptValue, err := strconv.Atoi(value); err == nil {
			attempt = attemptValue + 1
		}
	}

	updateOrAddHeader(msg, HeaderRetryAttempt, []byte(strconv.Itoa(attempt)))

	return attempt
}

func getNextEpoch(attempt int, strategy retry.Strategy) int64 {
	durationToAdd := strategy.DurationForAttempt(attempt)
	return Now().Add(durationToAdd).Unix()
}
