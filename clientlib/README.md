# clientlib

This module will helps you wrap a confluent.Message as scheduler message.
It will not send the message to the scheduler's topic, just preparing the message to be produced.

* To create a new schedule

```
import(
    "github.com/etf1/kafka-message-scheduler/clientlib"
)

key := "video1"
targetTopic := "target-topic"
schedulerTopic := "scheduler-topic"
scheduleID := "video1-schedule-online"
later := time.Now().Add(1 * time.Hour).Unix()

msg := kafka.Message{
    Key: bytes(key),
    Timestamp:     now,
    TimestampType: kafka.TimestampCreateTime,
    Value:         bytes("some value"),
    TopicPartition: kafka.TopicPartition{
        Topic: &targetTopic,
    },
}

result, err := clientlib.Schedule(&msg, scheduleID, later, schedulerTopic)

if err != nil {
    log.Printf("unexpected error: %v", err)
}
```

*  To delete a schedule

```
import(
    "github.com/etf1/kafka-message-scheduler/clientlib"
)

key := "video1"
targetTopic := "target-topic"
schedulerTopic := "scheduler-topic"
scheduleID := "video1-schedule-online"

msg := kafka.Message{
    Key: bytes(key),
    Timestamp:     now,
    TimestampType: kafka.TimestampCreateTime,
    Value:         bytes("some value"),
    TopicPartition: kafka.TopicPartition{
        Topic: &targetTopic,
    },
}

result, err := clientlib.DeleteSchedule(scheduleID, schedulerTopic)

if err != nil {
    log.Printf("unexpected error: %v", err)
}
```