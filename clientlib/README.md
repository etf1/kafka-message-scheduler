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

package main

import(
    "github.com/etf1/kafka-message-scheduler/clientlib"
)

func main() {
    schedulerMessage, err := clientlib.DeleteSchedule("video1-schedule-online", "scheduler-topic")
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

## Schedule a message to be retried

A Kafka message can also be scheduled as a retry object by applying a retry strategy. To do so, you can use the following `ScheduleRetry` function:

```go
msg := &kafka.Message{}

retryConfig := retry.NewConfig() // No strategy specified, will use the default one
result, err := clientlib.ScheduleRetry(msg, "schedule-id", "scheduler_topic", retryConfig)

// ...
```

Default strategy applied is a backoff exponential strategy with:
* Minimum retry value: 5 seconds
* Maximum retry value: 1 hour
* Factor: 2 (5 seconds, 10 seconds, 20 seconds, 40 seconds, ...)
* Maximum retry attempt: 50

A custom backoff strategy rule can be applied by using the following full example:

```go
package name

import (
    "github.com/etf1/kafka-message-scheduler/clientlib/retry"
)

func main() {
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

    retryConfig := retry.NewConfig(
        retry.WithMaxAttempt(20),
        retry.WithStrategy(&retry.Backoff{
            Min: 10 * time.Second,
            Max: 24 * time.Hour,
            Factor: 4,
        }),
    )

    schedulerMessage, err := clientlib.ScheduleRetry(&msg, "video1-schedule-online", "scheduler-topic", retryConfig)
    if err != nil {
        if err == ErrMaxAttemptReached {
            log.Print("message have been retried maximum number of time, sending to dead queue now...")
        } else {
            log.Printf("unexpected error: %v", err)
        }
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

### Define a custom retry strategy

You can define your own strategy by implementing the following interface (defined in the `retry` package):

```go
type Strategy interface {
	DurationForAttempt(attempt int) time.Duration
	Reset()
}
```


### Check if a message have been retried

You can also use this function to know if a message have already been retried at least once:

```go
msg := &kafka.Message{}
if clientlib.IsRetriedMessage(msg) {
    // Do something
}
```