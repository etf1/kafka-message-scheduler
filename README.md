![Go](https://github.com/etf1/kafka-message-scheduler/workflows/Go/badge.svg) [![Go Reference](https://pkg.go.dev/badge/github.com/etf1/kafka-message-scheduler.svg)](https://pkg.go.dev/github.com/etf1/kafka-message-scheduler) [![Go Report Card](https://goreportcard.com/badge/github.com/etf1/kafka-message-scheduler)](https://goreportcard.com/report/github.com/etf1/kafka-message-scheduler)

# Kafka message scheduler

Kafka message scheduler allows you to send message to a target topic on a specific time with a particular payload.

# Why ?

You always need to trigger events to do something and when you are using kafka in your company for data processing, you want to trigger this processing based on kafka message.

# Use cases

For example, on the TF1 website, videos are set online on a given date and set offline on another date.
We are using kafka consumers on specific topic to perform this activation/deactivation.

Another use case is retriable actions. For example when you are using messages of a topic for  performing action to an external API. This service can be down temporarily. So in this case failed messages can be rescheduled for a retry by the scheduler. Of course you have to make sure of idempotence, based on the original timestamp.

You can also imagine to use the scheduler to delete inactive user, each time a user logins you schedule an event with a date now + 1year. And if the user didn't login for a long time the scheduled message will be triggered and the user can be deleted by a specific consumer.

A lot of use cases can be found... it depends on your imagination ;)

# How does it work ?

Kafka message scheduler is simply using kafka topics. These topics contains all the schedules to trigger. These messages are regular kafka messages with headers and a payload. But it should contains specific headers:

* scheduler-epoch: the date of the schedule in epoch (number of second since 1970)
* scheduler-target-topic: the topic to send the message to
* scheduler-target-key: the key to use when sending the triggered schedule to the target topic

That is all you need, the paylaod will be the one defined in the schedule message.
Warning: if the payload of the message changes, a new schedule message should be send to the scheduler topic.

Example:

Schedule message:
```yaml
Headers:
    scheduler-epoch: 1893456000
    scheduler-target-topic: online-videos
    scheduler-target-key: vid1
    customer-header: dummy
Timestamp: 1607918336
Key: vid1-online
Value: "video 1"
```

Triggered message in topic 'online-videos':
```yaml
Headers:
    scheduler-timestamp: 1607918336 # original message timestamp
    scheduler-key: vid1-online
    scheduler-topic: schedules
    customer-header: dummy
Key: vid1
Value: "video 1"
```

The same message will also be produced to the 'history' topic for auditing.
Once the message is triggered a tombstone message (nil payload) will also be produced in the scheduler topic for deleting the schedule.

For GO there is a clientlib for wrapping your kafka messages, check [clientlib](clientlib/) for more details.

# High availability

You can launch multiple instance of the scheduler, schedules will be load balanced and for large schedules topic this will reduce the memory pressure on each scheduler instance. Scheduler can work with several incoming topics, but schedules must have uniq key accross all these topics.

# Fail over

If a scheduler crashed or is down for long period of time, it will resynch all schedules assigned to it and trigger missed schedules first as soon as possible and manage all live incoming schedules on the fly.

# Observability

The scheduler is exposing metrics on a specific port (8001 by default) at the URI /metrics. It can be used by a prometheus server for scrapping metrics. Available metrics are the number of missed, invalid, deleted, planned and triggered schedules. The metric name is `kafka_scheduler_event_total`.

# Configuration

The scheduler can be configured with environment variables:

| Env. Variable             | Default          | Description                                                                                  |
|---------------------------|------------------|----------------------------------------------------------------------------------------------|
| `CONFIGURATION_FILE`      |                  | Optional path to a YAML configuration file (see below)                                       |
| `BOOTSTRAP_SERVERS`       | `localhost:9092` | Kafka bootstrap servers list separated by comma                                              |
| `SCHEDULES_TOPICS`        | `schedules`      | Topic list for incoming schedules separated by comma                                         |
| `SINCE_DELTA`             | `0`              | Number of days to go back for considering missed schedules (0:today, -1: yesterday, etc ...) |
| `GROUP_ID`                | `scheduler-cg`   | Consumer group id for the scheduler consumer                                                 |
| `METRICS_HTTP_ADDR`       | `:8001`          | HTTP address where prometheus metrics will be exposed (URI /metrics)                         |
| `HISTORY_TOPIC`           | `history`        | Topic name where a copy of triggered schedules will be kept for auditing                     |
| `SCHEDULE_GRACE_INTERVAL` |                  | Interval to allow schedule with outdated epoch, grace interval: now-grace_interval and now   |

## Schedule grace interval
A number of second between 0 and n, if you want to allow outdated schedules with 3s (grace interval will be between now-3s and now), then set the value
to 3. By default the value is 0, so no grace interval.

## YAML configuration file

The `CONFIGURATION_FILE` environment variable specifies the path to a YAML file containing
additional configuration values:

```yaml
# kafka.common.configuration specifies a dictionary of librdkafka configuration values to use for
# both the consumer that reads from the schedule topics and the producer that writes to the target
# topics and history topic.  The key must be a valid configuration key that is applicable for both
# producers and consumers, and is specified in
# https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
#
# Values that conflict with the environment variables should be avoided.  For example, setting the
# "bootstrap.servers" option here will be ignored in favor of the bootstrap servers specified by the
# "BOOTSTRAP_SERVERS" environment variable (including the default value for the variable).
kafka.common.configuration:
    # Example values accepted by librdkafka producers and consumers:
    sasl.mechanisms: PLAIN
    sasl.username: cluster username
    sasl.password: cluster password
    security.protocol: SASL_SSL

# kafka.producer.configuration is the same as kafka.common.configuration, except these settings
# are only used for the producer and not the consumer.
kafka.producer.configuration:
    # Example values accepted by librdkafka producers:
    compression.codec: zstd
    partitioner: murmur2_random

# kafka.consumer.configuration is the same as kafka.common.configuration, except these settings
# are only used for the consumer and not the producer.
kafka.consumer.configuration:
    # Example values accepted by librdkafka consumers:
    allow.auto.create.topics: true
```

# Usage

You can use kafka message scheduler with docker or as code in your go program:

* go run :

```ruby
BOOTSTRAP_SERVERS="kafka:9092" go run ./cmd/kafka
```

* docker run :

```ruby
docker run -e BOOTSTRAP_SERVERS="kafka:9092" etf1/kafka-message-scheduler
```

* as code in your go program:

```go
import(
    runner "github.com/etf1/kafka-message-scheduler/runner/kafka"
)

...
kafkaRunner := runner.DefaultRunner()
go func(){
    if err := kafkaRunner.Start(); err != nil {
        log.Printf("failed to start scheduler: %v", err)
    }
}
...
kafkaRunner.Close()
```

# Schedules topics

The topic used by the scheduler has to be compacted and retention unlimited otherwise you will loose schedules. 
If you plan to run multiple instances of the scheduler for example 3, you need at least 3 partitions in the topic.

```ruby
kafka-topics --bootstrap-server "${BOOTSTRAP_SERVERS}" --create --topic schedules \
             --partitions 3 --config "cleanup.policy=compact" --config "retention.ms=-1"
```

# History topic

Triggered schedules are stored in the history topic specified by the env. variable `HISTORY_TOPIC`.

```ruby
kafka-topics --bootstrap-server "${BOOTSTRAP_SERVERS}" --create --topic history
```

# Scheduler mini

For integration tests, you can start a "mini" version of the scheduler which doesn't need any dependencies (no need for kafka).
This version is useful for integration tests with the scheduler rest api.

* go run :

```ruby
go run ./cmd/mini
```

* docker run :

```ruby
docker run etf1/kafka-message-scheduler:mini
```
# Admin GUI

An admin GUI is available on another git repository for viewing schedulers info and schedules. For more information: https://github.com/etf1/kafka-message-scheduler-admin
