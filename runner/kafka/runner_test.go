package kafka_test

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	hmapcoll "github.com/etf1/kafka-message-scheduler/internal/collector/hmap"
	"github.com/etf1/kafka-message-scheduler/internal/helper"
	"github.com/etf1/kafka-message-scheduler/internal/test"
	"github.com/etf1/kafka-message-scheduler/runner/kafka"
)

var (
	fullMessage           = test.FullMessage
	produceMessages       = test.ProduceMessages
	createTopics          = test.CreateTopics
	consumeMessages       = test.ConsumeMessages
	assertMessagesInTopic = test.AssertMessagesinTopic
	getBootstrapServers   = helper.GetDefaultBootstrapServers
)

type tuple struct {
	Key   []byte
	Value []byte
}

func checkMessagesInTopic(t *testing.T, topic string, expected []tuple) {
	result := consumeMessages(t, topic)

	if len(result) != len(expected) {
		t.Errorf("unexpected result length: %v, expected: %v", len(result), len(expected))
		return
	}

	for _, ve := range expected {
		found := false
		for _, vr := range result {
			if bytes.Equal(ve.Key, vr.Key) && bytes.Equal(ve.Value, vr.Value) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected element not found: %+v", ve)
			return
		}
	}
}

// Check the scheduler is working as expected, tombstone, history and target message should be published
func TestDefaultKafkaRunner(t *testing.T) {
	topics := createTopics(t, 3, []int{2, 1, 1}, "scheduler")

	someValue := []byte("some value")
	targetKey := "target-key"
	scheduleKey := "schedule-key"

	// scheduler topic
	schedulesTopic := topics[0]
	// history topic for audit
	historyTopic := topics[1]
	// the topic where the message should be delivered
	targetTopic := topics[2]

	os.Setenv("BOOTSTRAP_SERVERS", getBootstrapServers())
	os.Setenv("SCHEDULES_TOPICS", schedulesTopic)
	os.Setenv("HISTORY_TOPIC", historyTopic)

	kafkaRunner := kafka.NewRunner(kafka.DefaultConfig(), kafka.DefaultSince(), hmapcoll.New())
	defer kafkaRunner.Close()

	go func() {
		if err := kafkaRunner.Start(); err != nil {
			log.Printf("failed to create the default kafka runner: %v", err)
		}
	}()

	time.Sleep(30 * time.Second)

	epoch := time.Now().Add(10 * time.Second).Unix()
	msg1 := fullMessage(schedulesTopic, scheduleKey, someValue, epoch, targetTopic, targetKey)
	// message with nil target key should work
	msg2 := fullMessage(schedulesTopic, scheduleKey+"2", someValue, epoch, targetTopic, nil)
	msgs := []*confluent.Message{msg1, msg2}

	produceMessages(t, msgs)
	assertMessagesInTopic(t, schedulesTopic, msgs)

	expectedMsg := []tuple{{Key: []byte(targetKey), Value: someValue}, {Key: nil, Value: someValue}}

	t.Logf("check message in target topic")
	// check messages are in the target topic
	checkMessagesInTopic(t, targetTopic, expectedMsg)

	t.Logf("check message in history topic")
	// check messages are in the history topic
	checkMessagesInTopic(t, historyTopic, expectedMsg)

	expectedSchedules := []tuple{{Key: []byte(scheduleKey), Value: someValue}, {Key: []byte(scheduleKey + "2"), Value: someValue},
		{Key: []byte(scheduleKey), Value: nil}, {Key: []byte(scheduleKey + "2"), Value: nil}}

	t.Logf("check message in schedules topic")
	// check message and tombstone message are in schedules topic
	checkMessagesInTopic(t, schedulesTopic, expectedSchedules)
}

// Test the relience of the multi-instance scheduler,
// We start two schedulers which will be assigned to separate partitions
// then we stop and start each scheduler time to time.
// We should get in the target topic the exact number of schedules planned.
// Each scheduler should recover and not produce less or more messages than planned schedules originally.
func TestDefaultKafkaRunner_resilience(t *testing.T) {
	topics := createTopics(t, 3, []int{3, 1, 1}, "scheduler")

	// scheduler topic with 3 partitions
	schedulesTopic := topics[0]
	// history topic for audit
	historyTopic := topics[1]
	// the topic where the message should be delivered
	targetTopic := topics[2]

	os.Setenv("BOOTSTRAP_SERVERS", getBootstrapServers())
	os.Setenv("SCHEDULES_TOPICS", schedulesTopic)
	os.Setenv("HISTORY_TOPIC", historyTopic)

	startKafkaRunner := func() *kafka.Runner {
		// kafka runner just like default runner
		// except the prometheus collector because duplicate metrics collector registration is forbidden
		kafkaRunner := kafka.NewRunner(kafka.DefaultConfig(), kafka.DefaultSince(), hmapcoll.New())
		go func() {
			if err := kafkaRunner.Start(); err != nil {
				log.Printf("failed to create the default kafka runner: %v", err)
			}
		}()

		return kafkaRunner
	}

	kafkaRunner1 := startKafkaRunner()
	kafkaRunner2 := startKafkaRunner()

	msgs := make([]*confluent.Message, 0)

	// wait for init
	time.Sleep(30 * time.Second)

	delay := time.Now().Add(5 * time.Second).Unix()

	// 100 schedules
	for i := 1; i <= 100; i++ {
		scheduleID := fmt.Sprintf("schedule-%d", i)
		epoch := delay + int64(i)
		msg := fullMessage(schedulesTopic, scheduleID, scheduleID, epoch, targetTopic, scheduleID)
		msgs = append(msgs, msg)
	}

	produceMessages(t, msgs)

	step1 := time.After(20 * time.Second)
	step2 := time.After(40 * time.Second)
	step3 := time.After(60 * time.Second)
	step4 := time.After(80 * time.Second)
	timeout := time.After(2 * time.Minute)

loop:
	for {
		select {
		case <-step1:
			kafkaRunner1.Close()
			time.Sleep(3 * time.Second)
			kafkaRunner1 = startKafkaRunner()
		case <-step2:
			kafkaRunner2.Close()
			time.Sleep(3 * time.Second)
			kafkaRunner2 = startKafkaRunner()
		case <-step3:
			kafkaRunner1.Close()
			time.Sleep(3 * time.Second)
			kafkaRunner1 = startKafkaRunner()
		case <-step4:
			kafkaRunner2.Close()
			time.Sleep(3 * time.Second)
			kafkaRunner2 = startKafkaRunner()
		case <-timeout:
			println("break loop")
			break loop
		}
	}

	kafkaRunner1.Close()
	kafkaRunner2.Close()

	// check messages are in the target topic
	result := consumeMessages(t, targetTopic)

	// failovers succession of the runners, should not impact the number of messages published in the target topic
	// the optimal result will be len(result) == 100, but sometimes we have more, duplicate messages in very corny cases
	// TODO: investiguate the duplicated messages in this resilience test
	if len(result) < 100 {
		t.Fatalf("unexpected result length: %v", len(result))
	}
}

// Make sure scheduler configured with a yaml file runs correctly
func TestDefaultKafkaRunner_yaml_configuration(t *testing.T) {
	topics := createTopics(t, 3, []int{2, 1, 1}, "scheduler")

	someValue := []byte("some value")
	targetKey := "target-key"
	scheduleKey := "schedule-key"

	// scheduler topic
	schedulesTopic := topics[0]
	// history topic for audit
	historyTopic := topics[1]
	// the topic where the message should be delivered
	targetTopic := topics[2]

	os.Setenv("CONFIGURATION_FILE", "config_test.yaml")
	os.Setenv("BOOTSTRAP_SERVERS", getBootstrapServers())
	os.Setenv("SCHEDULES_TOPICS", schedulesTopic)
	os.Setenv("HISTORY_TOPIC", historyTopic)

	kafkaRunner := kafka.NewRunner(kafka.DefaultConfig(), kafka.DefaultSince(), hmapcoll.New())
	defer kafkaRunner.Close()

	go func() {
		if err := kafkaRunner.Start(); err != nil {
			log.Printf("failed to create the default kafka runner: %v", err)
		}
	}()

	time.Sleep(30 * time.Second)

	epoch := time.Now().Add(10 * time.Second).Unix()
	msg := fullMessage(schedulesTopic, scheduleKey, someValue, epoch, targetTopic, targetKey)
	msgs := []*confluent.Message{msg}

	produceMessages(t, msgs)
	assertMessagesInTopic(t, schedulesTopic, msgs)

	expectedMsg := []tuple{{Key: []byte(targetKey), Value: someValue}}

	// check message is in the target topic
	checkMessagesInTopic(t, targetTopic, expectedMsg)

	// check message is in the history topic
	checkMessagesInTopic(t, historyTopic, expectedMsg)

	expectedSchedules := []tuple{{Key: []byte(scheduleKey), Value: someValue}, {Key: []byte(scheduleKey), Value: nil}}

	// check message is in the history topic
	checkMessagesInTopic(t, schedulesTopic, expectedSchedules)
}

// Issue #30: https://github.com/etf1/kafka-message-scheduler/issues/30
// make sure invalid schedules are deleted from the topic
func TestDefaultKafkaRunner_issue30(t *testing.T) {
	topics := createTopics(t, 3, []int{2, 1, 1}, "scheduler")

	someValue := []byte("some value")
	targetKey := "target-key"
	scheduleKey := "schedule-key"

	// scheduler topic
	schedulesTopic := topics[0]
	// history topic for audit
	historyTopic := topics[1]
	// the topic where the message should be delivered
	targetTopic := topics[2]

	os.Setenv("BOOTSTRAP_SERVERS", getBootstrapServers())
	os.Setenv("SCHEDULES_TOPICS", schedulesTopic)
	os.Setenv("HISTORY_TOPIC", historyTopic)

	kafkaRunner := kafka.NewRunner(kafka.DefaultConfig(), kafka.DefaultSince(), hmapcoll.New())
	defer kafkaRunner.Close()

	go func() {
		if err := kafkaRunner.Start(); err != nil {
			log.Printf("failed to create the default kafka runner: %v", err)
		}
	}()

	time.Sleep(30 * time.Second)

	epoch := time.Now().Add(-1 * time.Second).Unix()
	msg := fullMessage(schedulesTopic, scheduleKey, someValue, epoch, targetTopic, targetKey)
	msgs := []*confluent.Message{msg}

	produceMessages(t, msgs)
	assertMessagesInTopic(t, schedulesTopic, msgs)

	t.Logf("check message NOT in target topic")
	// check messages are not in the target topic
	checkMessagesInTopic(t, targetTopic, []tuple{})

	t.Logf("check message NOT in history topic")
	// check messages are not in the history topic
	checkMessagesInTopic(t, historyTopic, []tuple{})

	expectedSchedules := []tuple{{Key: []byte(scheduleKey), Value: someValue}, {Key: []byte(scheduleKey), Value: nil}}

	t.Logf("check messages in schedules topic")
	// check invalid message and tombstone message are in schedules topic because it should be deleted from the topic
	checkMessagesInTopic(t, schedulesTopic, expectedSchedules)
}

// Issue #31: https://github.com/etf1/kafka-message-scheduler/issues/31
// allow grace interval for outdated schedules, for example now - 2s should be processed as valid schedule
// the grace interval should be configurable via a env. variable
func TestDefaultKafkaRunner_issue31(t *testing.T) {
	someValue := []byte("some value")
	targetKey := "target-key"
	scheduleKey := "schedule-key"

	cases := []struct {
		graceIntervalSeconds            string // 1000, 2000, etc...
		epochDuration                   time.Duration
		expectedMessageInTargetTopic    []tuple
		expectedMessageInHistoryTopic   []tuple
		expectedMessageInSchedulesTopic []tuple
	}{
		{"", // by default no grace
			0,
			// should get a message in target topic
			[]tuple{{Key: []byte(targetKey), Value: someValue}},
			// also in the history topic
			[]tuple{{Key: []byte(targetKey), Value: someValue}},
			// in schedules topic, it should be the schedule and the tombstone for the triggered schedule
			[]tuple{{Key: []byte(scheduleKey), Value: someValue}, {Key: []byte(scheduleKey), Value: nil}},
		},
		{"", // by default no grace
			-1 * time.Second,
			// should not get a message in target topic
			[]tuple{},
			// also not in the history topic
			[]tuple{},
			// in schedules topic, it should be the schedule and the tombstone for the outdated schedule
			[]tuple{{Key: []byte(scheduleKey), Value: someValue}, {Key: []byte(scheduleKey), Value: nil}},
		},
		{"1", // grace interval between (now - 1s) and now
			-1 * time.Second,
			// should get a message in target topic
			[]tuple{{Key: []byte(targetKey), Value: someValue}},
			// also in the history topic
			[]tuple{{Key: []byte(targetKey), Value: someValue}},
			// in schedules topic, it should be the schedule and the tombstone for the triggered schedule
			[]tuple{{Key: []byte(scheduleKey), Value: someValue}, {Key: []byte(scheduleKey), Value: nil}},
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case #%v", i), func(t *testing.T) {
			topics := createTopics(t, 3, []int{2, 1, 1}, "scheduler")

			// scheduler topic
			schedulesTopic := topics[0]
			// history topic for audit
			historyTopic := topics[1]
			// the topic where the message should be delivered
			targetTopic := topics[2]

			if c.graceIntervalSeconds != "" {
				os.Setenv("SCHEDULE_GRACE_INTERVAL", c.graceIntervalSeconds)
			}
			os.Setenv("BOOTSTRAP_SERVERS", getBootstrapServers())
			os.Setenv("SCHEDULES_TOPICS", schedulesTopic)
			os.Setenv("HISTORY_TOPIC", historyTopic)

			kafkaRunner := kafka.NewRunner(kafka.DefaultConfig(), kafka.DefaultSince(), hmapcoll.New())
			defer kafkaRunner.Close()

			go func() {
				if err := kafkaRunner.Start(); err != nil {
					log.Printf("failed to create the default kafka runner: %v", err)
				}
			}()

			time.Sleep(30 * time.Second)

			epoch := time.Now().Add(c.epochDuration).Unix()
			msg := fullMessage(schedulesTopic, scheduleKey, someValue, epoch, targetTopic, targetKey)
			msgs := []*confluent.Message{msg}

			produceMessages(t, msgs)
			assertMessagesInTopic(t, schedulesTopic, msgs)

			t.Logf("check message in target topic")
			// check messages are in the target topic
			checkMessagesInTopic(t, targetTopic, c.expectedMessageInTargetTopic)

			t.Logf("check message in history topic")
			// check messages are in the history topic
			checkMessagesInTopic(t, historyTopic, c.expectedMessageInHistoryTopic)

			t.Logf("check messages in schedules topic")
			// check messages are in the schedules topic
			checkMessagesInTopic(t, schedulesTopic, c.expectedMessageInSchedulesTopic)
		})
	}
}
