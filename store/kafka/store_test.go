package kafka_test

import (
	"testing"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/etf1/kafka-message-scheduler/internal/test"
	"github.com/etf1/kafka-message-scheduler/schedule"
	kafka_schedule "github.com/etf1/kafka-message-scheduler/schedule/kafka"
	"github.com/etf1/kafka-message-scheduler/store"
)

var (
	newKafkaStore           = test.NewKafkaStore
	newKafkaStoreFromTopics = test.NewKafkaStoreFromTopics
	message                 = test.Message
	messages                = test.Messages
	produceMessages         = test.ProduceMessages
	createTopics            = test.CreateTopics
)

// Rule #1: valid schedules should be collected
func TestStore_events(t *testing.T) {
	now := time.Now()
	epochs := make([]int64, 0)

	for i := 1; i <= 10; i++ {
		epochs = append(epochs, now.Add(1*time.Second).Unix())
	}

	kstore, topics := newKafkaStore(t, 1, []int{2})
	defer func() {
		err := kstore.Close()
		if err != nil {
			t.Fatalf("unexpected error on close: %v", err)
		}
	}()

	msgs := messages(topics[0], epochs)
	produceMessages(t, msgs)

	events := kstore.Events()

	live := messages(topics[0], epochs)
	msgs = append(msgs, live...)

	go func() {
		produceMessages(t, live)
	}()

	result := make([]store.Event, 0)
loop:
	for {
		select {
		case evt := <-events:
			t.Logf("received %T %v\n", evt, evt)
			result = append(result, evt)
		case <-time.After(15 * time.Second):
			break loop
		}
	}

	if len(result) != len(msgs) {
		t.Fatalf("unexpected length, expected %v got %v", len(msgs), len(result))
	}

	for i, evt := range result {
		t.Logf("%v: %T %v", i, evt, evt)
	}

	schedulesFromResult := make([]schedule.Schedule, 0, len(result))
	for _, evt := range result {
		sch, ok := evt.(schedule.Schedule)
		if !ok {
			t.Fatalf("unexpected type: %T %v", evt, evt)
		}
		schedulesFromResult = append(schedulesFromResult, sch)
	}

	schedulesFromMsgs := make([]schedule.Schedule, 0, len(msgs))
	for _, msg := range msgs {
		schedulesFromMsgs = append(schedulesFromMsgs, kafka_schedule.Schedule{
			Message: msg,
		})
	}

	if !schedule.AreSame(schedulesFromMsgs, schedulesFromResult) {
		t.Fatalf("unexpected result %v != %v", schedulesFromMsgs, schedulesFromResult)
	}
}

// Rule #2: invalid schedules should be collected
func TestStore_invalid(t *testing.T) {
	now := time.Now()

	kstore, topics := newKafkaStore(t, 1, []int{2})
	defer func() {
		err := kstore.Close()
		if err != nil {
			t.Fatalf("unexpected error on close: %v", err)
		}
	}()

	invalids := []*confluent.Message{
		message(topics[0], "", "value", now.Add(-10*time.Second).Unix()),
		message(topics[0], " ", "value", now.Add(10*time.Second).Unix()),
		message(topics[0], "  ", "value", now.Unix()),
		message(topics[0], "1", "value", -1),
	}

	produceMessages(t, invalids)

	events := kstore.Events()

	invalidsLive := []*confluent.Message{
		message(topics[0], "", "value", now.Add(-10*time.Second).Unix()),
		message(topics[0], " ", "value", now.Add(10*time.Second).Unix()),
		message(topics[0], "  ", "value", 0),
		message(topics[0], "1", "value", -1),
	}
	invalids = append(invalids, invalidsLive...)

	go func() {
		produceMessages(t, invalidsLive)
	}()

	result := make([]store.Event, 0)
loop:
	for {
		select {
		case evt := <-events:
			t.Logf("received %T %v\n", evt, evt)
			result = append(result, evt)
		case <-time.After(15 * time.Second):
			break loop
		}
	}

	for i, evt := range result {
		t.Logf("%v: %T %v", i, evt, evt)
	}

	invalidsFromResult := make([]schedule.Schedule, 0)

	for _, evt := range result {
		switch sch := evt.(type) {
		case schedule.InvalidSchedule:
			invalidsFromResult = append(invalidsFromResult, sch)
		default:
			t.Fatalf("unexpected schedule type %T %v", evt, evt)
		}
	}

	// invalids
	schedulesFromMsgs := make([]schedule.Schedule, 0)
	for _, msg := range invalids {
		schedulesFromMsgs = append(schedulesFromMsgs, kafka_schedule.Schedule{
			Message: msg,
		})
	}

	if !schedule.AreSame(invalidsFromResult, schedulesFromMsgs) {
		t.Fatalf("unexpected result %v != %v", invalidsFromResult, schedulesFromMsgs)
	}
}

// Rule #3: deleted schedules should be collected
func TestStore_delete(t *testing.T) {
	now := time.Now()

	kstore, topics := newKafkaStore(t, 1, []int{2})
	defer func() {
		err := kstore.Close()
		if err != nil {
			t.Fatalf("unexpected error on close: %v", err)
		}
	}()

	deletes := []*confluent.Message{
		// delete message, epoch doesn't matter
		message(topics[0], "1", nil, now.Add(-48*time.Hour).Unix()),
		message(topics[0], "2", nil, now.Add(48*time.Hour).Unix()),
		message(topics[0], "3", nil, 0),
	}

	produceMessages(t, deletes)

	events := kstore.Events()

	go func() {
		deletesLive := []*confluent.Message{
			// delete message, epoch doesn't matter
			message(topics[0], "4", nil, now.Add(-48*time.Hour).Unix()),
			message(topics[0], "5", nil, now.Add(48*time.Hour).Unix()),
			message(topics[0], "6", nil, 0),
		}
		deletes = append(deletes, deletesLive...)

		produceMessages(t, deletesLive)
	}()

	result := make([]store.Event, 0)
loop:
	for {
		select {
		case evt := <-events:
			t.Logf("received %T %v\n", evt, evt)
			result = append(result, evt)
		case <-time.After(15 * time.Second):
			break loop
		}
	}

	for i, evt := range result {
		t.Logf("%v: %T %v", i, evt, evt)
	}

	deletesFromResult := make([]schedule.Schedule, 0)

	for _, evt := range result {
		switch sch := evt.(type) {
		case schedule.DeletedSchedule:
			deletesFromResult = append(deletesFromResult, sch)
		default:
			t.Fatalf("unexpected schedule type %T %v", evt, evt)
		}
	}

	// deletes
	schedulesFromMsgs := make([]schedule.Schedule, 0)
	for _, msg := range deletes {
		schedulesFromMsgs = append(schedulesFromMsgs, kafka_schedule.Schedule{
			Message: msg,
		})
	}

	if !schedule.AreSame(deletesFromResult, schedulesFromMsgs) {
		t.Fatalf("unexpected result %v != %v", deletesFromResult, schedulesFromMsgs)
	}
}

// Rule #4: DeleteSchedules should be triggered when needed (i.e when partitions are reassigned in kafka store)
func TestStore_delete_func(t *testing.T) {
	// create 1 topic with 2 partitions
	topics := createTopics(t, 1, []int{2}, "scheduler")

	// First store will be assigned to the existing two partitions of the topics
	// Then we create a second store and it will be assigned to one of the two partitions
	// This should trigger from the first store an event DeleteSchedules for the "unassigned" partition (0 or 1)
	kstore1 := newKafkaStoreFromTopics(t, topics)
	defer func() {
		err := kstore1.Close()
		println("close store1")
		if err != nil {
			t.Fatalf("unexpected error on close: %v", err)
		}
	}()
	events1 := kstore1.Events()

	// wait for the first store to receive assignment for the partitions
	time.Sleep(5 * time.Second)

	kstore2 := newKafkaStoreFromTopics(t, topics)
	defer func() {
		err := kstore2.Close()
		println("close store2")
		if err != nil {
			t.Fatalf("unexpected error on close: %v", err)
		}
	}()
	events2 := kstore2.Events()

	result := make([]store.Event, 0)
loop:
	for {
		select {
		case evt, open := <-events2:
			t.Logf("received events2 %T %v\n", evt, evt)
			if open {
				result = append(result, evt)
			}
		case evt, open := <-events1:
			t.Logf("received events %T %v\n", evt, evt)
			if open {
				result = append(result, evt)
			}
		case <-time.After(15 * time.Second):
			t.Logf("timeout")
			break loop
		}
	}

	if len(result) != 1 {
		t.Fatalf("unexpected result length: %v", len(result))
	}

	deleteSchedules, ok := result[0].(schedule.DeleteSchedules)
	if !ok {
		t.Fatalf("unexpected schedule type %T", result[0])
	}

	partition0 := deleteSchedules.DeleteFunc(kafka_schedule.Schedule{
		Message: &confluent.Message{
			TopicPartition: confluent.TopicPartition{
				Topic:     &topics[0],
				Partition: 0,
			},
		},
	})

	partition1 := deleteSchedules.DeleteFunc(kafka_schedule.Schedule{
		Message: &confluent.Message{
			TopicPartition: confluent.TopicPartition{
				Topic:     &topics[0],
				Partition: 1,
			},
		},
	})

	t.Logf("partition0=%v partition1=%v", partition0, partition1)

	// should be one partition only
	if partition0 == false && partition1 == false || partition0 == true && partition1 == true {
		t.Fatalf("DeleteFunc should delete schedules from one partition only")
	}
}
