# clientlib

This module will helps you wrap a confluent.Message as scheduler message.
It will not send the message to the scheduler's topic, just preparing the message to be produced.

## To create or update a scheduled message

```go
package main

import(
    "github.com/etf1/kafka-message-scheduler/clientlib"
)

func main() {
    targetTopic := "target-topic"

    now := time.Now()
    later := now.Add(1 * time.Hour).Unix()

    msg := kafka.Message{
        Key: []byte("video1"),
        Timestamp:     now,
        TimestampType: kafka.TimestampCreateTime,
        Value:         []byte("some value"),
        TopicPartition: kafka.TopicPartition{
            Topic: &targetTopic,
        },
    }

    schedulerMessage, err := clientlib.Schedule(&msg, "video1-schedule-online", later, "scheduler-topic")
    if err != nil {
        log.Printf("unexpected error: %v", err)
    }

    producer, err := kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers": "localhost:9092",
    })
    if err != nil {
        log.Printf("error while initializing producer: %v", err)
    }

    producer.Produce(schedulerMessage, nil)    
    producer.Close()
}
```

##  To delete a scheduled message

```go
targetTopic := "target-topic"

msg := kafka.Message{
    Key: []byte("video1"),
    Timestamp:     time.Now(),
    TimestampType: kafka.TimestampCreateTime,
    Value:         []byte("some value"),
    TopicPartition: kafka.TopicPartition{
        Topic: &targetTopic,
    },
}

result, err := clientlib.DeleteSchedule("video1-schedule-online", "scheduler-topic")
if err != nil {
    log.Printf("unexpected error: %v", err)
}
```
