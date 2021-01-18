package clientlib_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/etf1/kafka-message-scheduler/clientlib"
)

const (
	headerKey      = "header-key"
	headerValue    = "header value"
	schedulerTopic = "scheduler-topic"
	scheduleID     = "scheduler-id"
)

func bytes(i string) []byte {
	return []byte(i)
}

func checkHeader(t *testing.T, msg *kafka.Message, key, value string) {
	val, found := getHeaderValue(msg, key)
	if !found || val != value {
		t.Fatalf("unexpected header value : not found or value != %q", val)
	}
}

func getHeaderValue(msg *kafka.Message, key string) (string, bool) {
	for _, header := range msg.Headers {
		if header.Key == key && len(header.Value) > 0 {
			return string(header.Value), true
		}
	}
	return "", false
}

func message() *kafka.Message {
	key := "key"
	msgValue := "some value"
	targetTopic := "target-topic"

	msg := kafka.Message{
		Key: bytes(key),
		Headers: []kafka.Header{
			{
				Key:   headerKey,
				Value: bytes(headerValue),
			},
		},
		Timestamp:     time.Now(),
		TimestampType: kafka.TimestampCreateTime,
		Value:         bytes(msgValue),
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

	result, err := clientlib.Schedule(msg, scheduleID, later, schedulerTopic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(result.Key) != scheduleID {
		t.Fatalf("unexpected message key: %v", string(result.Key))
	}

	if string(result.Value) != string(msg.Value) {
		t.Fatalf("unexpected message value: %v", string(result.Value))
	}

	if *result.TopicPartition.Topic != schedulerTopic {
		t.Fatalf("unexpected message topic: %v", *result.TopicPartition.Topic)
	}

	checkHeader(t, result, headerKey, headerValue)
	checkHeader(t, result, clientlib.Epoch, strconv.FormatInt(later, 10))
	checkHeader(t, result, clientlib.TargetTopic, *msg.TopicPartition.Topic)
	checkHeader(t, result, clientlib.TargetKey, string(msg.Key))
}

// make sure that scheduler headers are not stacked when creating a schedule message from a message which has already these headers
func TestScheduleMessage_reschedule_check_headers(t *testing.T) {
	now := time.Now()
	later := now.Add(1 * time.Hour).Unix()

	msg := message()

	result, err := clientlib.Schedule(msg, scheduleID, later, schedulerTopic)
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
		Value: bytes(key),
	})

	msg = &kafka.Message{
		Key:           bytes(key),
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

	result, err = clientlib.Schedule(msg, scheduleID, later, schedulerTopic)
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
			_, err := clientlib.Schedule(&msg, c.scheduleID, c.epoch, c.schedulerTopic)
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

	result, err := clientlib.Schedule(msg, scheduleID, later, schedulerTopic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("msg.Headers=%v", len(msg.Headers))
	t.Logf("result.Headers=%v", len(result.Headers))

	if len(msg.Headers) != 1 {
		t.Fatalf("original message headers should not be modified, got %v", msg.Headers)
	}
}

func TestDeleteSchedule(t *testing.T) {
	schedulerTopic := "scheduler-topic"
	scheduleID := "scheduler-id"

	result, err := clientlib.DeleteSchedule(scheduleID, schedulerTopic)
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

	if clientlib.IsSchedulerMessage(msg) == true {
		t.Fatalf("unexpected result, should be false")
	}

	msg.Headers = append(msg.Headers, kafka.Header{
		Key:   "scheduler-key",
		Value: bytes("video-offline"),
	},
	)

	if clientlib.IsSchedulerMessage(msg) != true {
		t.Fatalf("unexpected result, should be true")
	}
}
