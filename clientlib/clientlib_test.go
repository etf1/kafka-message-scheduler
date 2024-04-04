package clientlib

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/etf1/kafka-message-scheduler/clientlib/retry"
)

const (
	headerKey      = "header-key"
	headerValue    = "header value"
	schedulerTopic = "scheduler-topic"
	scheduleID     = "scheduler-id"
)

func asBytes(i string) []byte {
	return []byte(i)
}

func checkHeader(t *testing.T, msg *kafka.Message, key, value string) {
	val, found := getHeaderValue(msg, key)
	if !found || val != value {
		t.Fatalf("unexpected header value : not found or value != %q (expected: %q)", val, value)
	}
}

func message() *kafka.Message {
	key := "key"
	msgValue := "some value"
	targetTopic := "target-topic"

	msg := kafka.Message{
		Key: asBytes(key),
		Headers: []kafka.Header{
			{
				Key:   headerKey,
				Value: asBytes(headerValue),
			},
		},
		Timestamp:     time.Now(),
		TimestampType: kafka.TimestampCreateTime,
		Value:         asBytes(msgValue),
		TopicPartition: kafka.TopicPartition{
			Topic: &targetTopic,
		},
	}

	return &msg
}

func TestScheduleMessage(t *testing.T) {
	now := time.Now()
	later := now.Add(1 * time.Hour).Unix()

	msg := message()

	result, err := Schedule(msg, scheduleID, later, schedulerTopic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(result.Key) != scheduleID {
		t.Fatalf("unexpected message key: %v", string(result.Key))
	}

	if !bytes.Equal(result.Value, msg.Value) {
		t.Fatalf("unexpected message value: %v", string(result.Value))
	}

	if *result.TopicPartition.Topic != schedulerTopic {
		t.Fatalf("unexpected message topic: %v", *result.TopicPartition.Topic)
	}

	checkHeader(t, result, headerKey, headerValue)
	checkHeader(t, result, HeaderEpoch, strconv.FormatInt(later, 10))
	checkHeader(t, result, HeaderTargetTopic, *msg.TopicPartition.Topic)
	checkHeader(t, result, HeaderTargetKey, string(msg.Key))
}

// make sure that scheduler headers are not stacked when creating a schedule message from a message which has already these headers
func TestScheduleMessage_reschedule_check_headers(t *testing.T) {
	now := time.Now()
	later := now.Add(1 * time.Hour).Unix()

	msg := message()

	result, err := Schedule(msg, scheduleID, later, schedulerTopic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Headers) != 4 {
		t.Fatalf("unexpected headers length, should be 4, got %v", len(result.Headers))
	}

	key := "video-offline"

	// reschedule message from scheduler
	headers := result.Headers
	headers = append(headers, kafka.Header{
		Key:   "scheduler-key",
		Value: asBytes(key),
	})

	msg = &kafka.Message{
		Key:           asBytes(key),
		Headers:       headers,
		Timestamp:     now,
		TimestampType: kafka.TimestampCreateTime,
		Value:         msg.Value,
		TopicPartition: kafka.TopicPartition{
			Topic: msg.TopicPartition.Topic,
		},
	}

	// existing headers + "scheduler-key" header => so 5 headers
	if len(msg.Headers) != 5 {
		t.Fatalf("unexpected headers length, should be 5, got %v", len(msg.Headers))
	}

	result, err = Schedule(msg, scheduleID, later, schedulerTopic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Headers) != 4 {
		t.Fatalf("unexpected headers length, should be 4, got %v", len(result.Headers))
	}
}

func TestScheduleMessage_invalid_parameters(t *testing.T) {
	later := time.Now().Add(1 * time.Hour).Unix()
	before := time.Now().Add(-1 * time.Hour).Unix()

	msg := kafka.Message{}

	cases := []struct {
		schedulerTopic string
		scheduleID     string
		epoch          int64
	}{
		{"", "schedule-id", later},
		{"scheduler-topic", "", later},
		{"scheduler-topic", "schedule-id", before},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("test case #%v", i), func(t *testing.T) {
			_, err := Schedule(&msg, c.scheduleID, c.epoch, c.schedulerTopic)
			if err == nil {
				t.Fatalf("error should not be nil")
			}
		})
	}
}

func TestScheduleMessage_should_not_modify_headers(t *testing.T) {
	now := time.Now()
	later := now.Add(1 * time.Hour).Unix()

	msg := message()

	result, err := Schedule(msg, scheduleID, later, schedulerTopic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("msg.Headers=%v", len(msg.Headers))
	t.Logf("result.Headers=%v", len(result.Headers))

	if len(msg.Headers) != 1 {
		t.Fatalf("original message headers should not be modified, got %v", msg.Headers)
	}
}

func TestScheduleRetry(t *testing.T) {
	Now = func() time.Time {
		return time.Date(2021, time.January, 27, 10, 0, 0, 0, time.UTC)
	}

	msg := message()

	retryConfig := retry.NewConfig()

	result, err := ScheduleRetry(msg, scheduleID, schedulerTopic, retryConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(result.Key) != scheduleID {
		t.Fatalf("unexpected message key: %v", string(result.Key))
	}

	if !bytes.Equal(result.Value, msg.Value) {
		t.Fatalf("unexpected message value: %v", string(result.Value))
	}

	if *result.TopicPartition.Topic != schedulerTopic {
		t.Fatalf("unexpected message topic: %v", *result.TopicPartition.Topic)
	}

	checkHeader(t, result, headerKey, headerValue)
	checkHeader(t, result, HeaderEpoch, strconv.FormatInt(1611741610, 10))
	checkHeader(t, result, HeaderTargetTopic, *msg.TopicPartition.Topic)
	checkHeader(t, result, HeaderTargetKey, string(msg.Key))

	checkHeader(t, result, HeaderRetryAttempt, strconv.Itoa(1))
	checkHeader(t, result, HeaderRetryOriginalTimestamp, strconv.FormatInt(Now().Unix(), 10))
}

func TestScheduleRetry_not_first_attempt(t *testing.T) {
	Now = func() time.Time {
		return time.Date(2021, time.January, 27, 10, 0, 0, 0, time.UTC)
	}

	msg := message()

	// Set current retry attempt counter to 10 and the original timestamp
	msg.Headers = append(msg.Headers, kafka.Header{
		Key:   HeaderRetryAttempt,
		Value: []byte(strconv.Itoa(10)),
	}, kafka.Header{
		Key:   HeaderRetryOriginalTimestamp,
		Value: []byte("1611542079"),
	})

	retryConfig := retry.NewConfig()

	result, err := ScheduleRetry(msg, scheduleID, schedulerTopic, retryConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(result.Key) != scheduleID {
		t.Fatalf("unexpected message key: %v", string(result.Key))
	}

	if !bytes.Equal(result.Value, msg.Value) {
		t.Fatalf("unexpected message value: %v", string(result.Value))
	}

	if *result.TopicPartition.Topic != schedulerTopic {
		t.Fatalf("unexpected message topic: %v", *result.TopicPartition.Topic)
	}

	checkHeader(t, result, headerKey, headerValue)
	checkHeader(t, result, HeaderEpoch, strconv.FormatInt(1611745200, 10))
	checkHeader(t, result, HeaderTargetTopic, *msg.TopicPartition.Topic)
	checkHeader(t, result, HeaderTargetKey, string(msg.Key))

	checkHeader(t, result, HeaderRetryAttempt, strconv.Itoa(11))
	checkHeader(t, result, HeaderRetryOriginalTimestamp, strconv.FormatInt(1611542079, 10))
}

func TestScheduleRetry_no_topic_associated(t *testing.T) {
	Now = func() time.Time {
		return time.Date(2021, time.January, 27, 10, 0, 0, 0, time.UTC)
	}

	msg := message()

	emptyValue := ""
	msg.TopicPartition.Topic = &emptyValue

	retryConfig := retry.NewConfig()

	result, err := ScheduleRetry(msg, scheduleID, schedulerTopic, retryConfig)
	if result != nil {
		t.Fatalf("result should be nil but found the following value: %v", result)
	}
	if err != ErrNoTopicAssociated {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScheduleRetry_maximum_attempts_reached(t *testing.T) {
	Now = func() time.Time {
		return time.Date(2021, time.January, 27, 10, 0, 0, 0, time.UTC)
	}

	msg := message()

	// Set current retry attempt counter to 50 (maximum value specified in configuration)
	msg.Headers = append(msg.Headers, kafka.Header{
		Key:   HeaderRetryAttempt,
		Value: []byte(strconv.Itoa(50)),
	})

	retryConfig := retry.NewConfig()

	result, err := ScheduleRetry(msg, scheduleID, schedulerTopic, retryConfig)
	if result != nil {
		t.Fatalf("result should be nil but found the following value: %v", result)
	}
	if err != ErrMaxAttemptReached {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteSchedule(t *testing.T) {
	schedulerTopic := "scheduler-topic"
	scheduleID := "scheduler-id"

	result, err := DeleteSchedule(scheduleID, schedulerTopic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(result.Key) != scheduleID {
		t.Fatalf("unexpected key: %v", result.Key)
	}

	if *result.TopicPartition.Topic != schedulerTopic {
		t.Fatalf("unexpected topic: %v", *result.TopicPartition.Topic)
	}

	if len(result.Value) != 0 {
		t.Fatalf("unexpected value: %v", len(result.Value))
	}

	if len(result.Headers) != 0 {
		t.Fatalf("unexpected headers length: %v", len(result.Headers))
	}
}

func TestIsSchedulerMessage(t *testing.T) {
	msg := message()

	if IsSchedulerMessage(msg) == true {
		t.Fatalf("unexpected result, should be false")
	}

	msg.Headers = append(msg.Headers, kafka.Header{
		Key:   "scheduler-key",
		Value: asBytes("video-offline"),
	},
	)

	if IsSchedulerMessage(msg) != true {
		t.Fatalf("unexpected result, should be true")
	}
}
