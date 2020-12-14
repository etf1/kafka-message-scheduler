package clientlib_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/etf1/kafka-message-scheduler/clientlib"
)

func bytes(i string) []byte {
	return []byte(i)
}

func checkHeader(t *testing.T, msg *kafka.Message, key string, value string) {
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

func TestScheduleMessage(t *testing.T) {
	now := time.Now()
	later := now.Add(1 * time.Hour).Unix()

	key := "key"
	targetTopic := "target-topic"
	schedulerTopic := "scheduler-topic"
	scheduleID := "scheduler-id"
	headerKey := "header-key"
	headerValue := "header value"
	msgValue := "some value"

	msg := kafka.Message{
		Key: bytes(key),
		Headers: []kafka.Header{
			{
				Key:   headerKey,
				Value: bytes(headerValue),
			},
		},
		Timestamp:     now,
		TimestampType: kafka.TimestampCreateTime,
		Value:         bytes(msgValue),
		TopicPartition: kafka.TopicPartition{
			Topic: &targetTopic,
		},
	}

	result, err := clientlib.Schedule(&msg, scheduleID, later, schedulerTopic)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(result.Key) != scheduleID {
		t.Fatalf("unexpected message key: %v", string(result.Key))
	}

	if string(result.Value) != msgValue {
		t.Fatalf("unexpected message value: %v", string(result.Value))
	}

	if string(*result.TopicPartition.Topic) != schedulerTopic {
		t.Fatalf("unexpected message topic: %v", string(*result.TopicPartition.Topic))
	}

	checkHeader(t, result, headerKey, headerValue)
	checkHeader(t, result, clientlib.Epoch, strconv.FormatInt(later, 10))
	checkHeader(t, result, clientlib.TargetTopic, targetTopic)
	checkHeader(t, result, clientlib.TargetKey, key)
}

func TestScheduleMessage_invalid_parameters(t *testing.T) {
	later := time.Now().Add(1 * time.Hour).Unix()
	before := time.Now().Add(-1 * time.Hour).Unix()
	targetTopic := "target-topic"

	msg := kafka.Message{
		Value: bytes("some value"),
		TopicPartition: kafka.TopicPartition{
			Topic: &targetTopic,
		},
	}

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

// DeleteSchedule TO BE TESTED
