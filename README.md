# Kafka message scheduler

Kafka message scheduler allows you to send message to a target topic on a specific time with a particular payload.

# Why ?

You always need to trigger events in the future to do something and when you are using kafka in your company for data processing, you want to trigger this processing based on message received in a specific topic.

# Use cases

You can find many use cases, but you have one very trivial : trigger an event in the future.
For example, on the TF1 website videos are put online at a specific time and put offline at another specific time.
We are using kafka consumers on specific topic to perform this activation/deactivation.

Another use case is retriable actions. For example when you are consuming message of a topic and sending a specific action to an external API, this service can be down temporarily. So failed messages can be scheduled for the scheduler to be retried in the future. Of course you have to make sure of idempotence, based for example on the original timestamp.

You can also imagine to use the scheduler to delete inactive user, each time a user logins you put a message in the scheduler to be triggered in 1 year. And if the user didn't login for a long time the scheduled message will be triggered and the user deleted.

A lot of use cases can be found...depends on your imagination

# How does it work ?

Kafka message scheduler is simply based on incoming topics. These topics contains all the schedules to trigger. These messages are regular kafka messages with headers and a payload. But it should contains specific header:

* scheduler-epoch: the time in epoch (number of second since 1970) for the schedule
* scheduler-target-topic: the topic to send the message to
* scheduler-target-key: the key to use when sending the triggered message to the target topic

That is all you need, the paylaod will used as defined in the schedule message.
Warning: if the payload of the message changes, a new schedule message should be send to the scheduler topic.

Example:

Schedule message:
```
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
```
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

You can launch multiple instance of the scheduler, schedules will be load balanced and for large schedules topic reduce the memory pressure. Scheduler can work with several incoming schedules topics, but schedules must have uniq key accross all these topics.

# Fail over

If a scheduler crashed or is down for long period of time, it will resynch all schedules assigned to it and trigger missed schedules first as soon as possible and manage all live incoming schedules on the fly.

# Observability

The scheduler is exposing metrics on a specific port (8001 by default) at the URI /metrics. It can be used by a prometheus server for scrapping metrics. Available metrics are the number of missed, invalid, deleted, planned and triggered schedules.

# Configuration

The scheduler can be configured with environment variables:

| Env. Variable     | Default        | Description                                                                                  |
|-------------------|----------------|----------------------------------------------------------------------------------------------|
| BOOTSTRAP_SERVERS | localhost:9092 | Kafka bootstrap servers list separated by comma                                              |
| SCHEDULES_TOPICS  | schedules      | Topic list for incoming schedules separated by comma                                         |
| SINCE_DELTA       | 0              | Number of days to go back for considering missed schedules (0:today, -1: yesterday, etc ...) |
| GROUP_ID          | scheduler-cg   | Consumer group id for the scheduler consumer                                                 |
| METRICS_HTTP_PORT | 8001           | HTTP port where prometheus metrics will be exposed (URI /metrics)                            |
| HISTORY_TOPIC     | history        | Topic name where a copy of triggered schedules will be kept for auditing                     |

# Usage

You can use kafka message scheduler with docker or as code in your go program:

* docker run :

```
docker run -e BOOTSTRAP_SERVERS="kafka:9092" etf1/kafka-message-scheduler
```

* as code:

```
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

The topic used by the scheduler has to be compact and retention unlimited otherwise you will loose schedules. 
If you plan to run multiple instances of the scheduler for example 3, you need at least 3 partitions in the topic.

```
kafka-topics --bootstrap-server "${BOOTSTRAP_SERVERS}" --create --topic schedules \
             --partitions 3 --config "cleanup.policy=compact" --config "retention.ms=-1"
```