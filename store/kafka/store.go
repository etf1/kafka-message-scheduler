package kafka

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/etf1/kafka-message-scheduler/schedule"
	kafka_schedule "github.com/etf1/kafka-message-scheduler/schedule/kafka"
	"github.com/etf1/kafka-message-scheduler/store"
)

const (
	eventsChanBuffer = 10000
)

type Error struct {
	error
	timestamp int64
}

func (k Error) String() string {
	return k.Error()
}

func (k Error) Timestamp() int64 {
	return k.timestamp
}

type Store struct {
	*kafka.Consumer
	events        chan store.Event
	stopChan      chan bool
	resetTicker   resetTicker
	pollTimeoutMs int
	revoked       kafka.RevokedPartitions
	assigned      kafka.AssignedPartitions
}

func NewStore(kafkaConfiguration kafka.ConfigMap, bootstrapServers string, topics []string, groupID string, sessionTimeout int) (*Store, error) {
	finalCfg := make(kafka.ConfigMap, len(kafkaConfiguration))
	finalCfg["broker.address.family"] = "v4" // allow the user to override this in the configuration file
	for k, v := range kafkaConfiguration {
		finalCfg[k] = v
	}
	// these configuration options override the configuration file:
	finalCfg["bootstrap.servers"] = bootstrapServers
	finalCfg["group.id"] = groupID
	finalCfg["session.timeout.ms"] = sessionTimeout
	finalCfg["enable.auto.commit"] = false
	finalCfg["go.events.channel.enable"] = false
	finalCfg["go.application.rebalance.enable"] = true

	consumer, err := kafka.NewConsumer(&finalCfg)

	if err != nil {
		return nil, fmt.Errorf("cannot create kafka consumer for the store: %w", err)
	}
	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot subscribe to topics '%s': %w", topics, err)
	}

	resetTicker := newResetTicker()
	resetTicker.start()

	s := Store{
		Consumer:      consumer,
		stopChan:      make(chan bool),
		resetTicker:   resetTicker,
		pollTimeoutMs: 10000,
	}

	return &s, nil
}

// TODO remove return error
func (ks *Store) Close() error {
	defer log.Println("kafka store closed")
	log.Println("kafka store closing ...")

	// if ks.events is nil means that the go routine for events has never been launched
	// so no need to signal in the stopChan, just close the consumer
	if ks.events != nil {
		ks.stopChan <- true
		close(ks.events)
	}
	return ks.Consumer.Close()
}

func (ks *Store) processMessage() {
	e := ks.Poll(ks.pollTimeoutMs)

	if e == nil {
		return
	}

	switch evt := e.(type) {
	case *kafka.Message:
		// wrap kafka message
		sch := kafka_schedule.Schedule{
			Message: evt,
		}
		errs := schedule.CheckSchedule(sch)
		if len(errs) > 0 {
			ks.events <- schedule.InvalidSchedule{
				Schedule: sch,
				Errors:   errs,
			}
			break
		}
		if sch.IsDeleted() {
			ks.events <- schedule.DeletedSchedule{
				Schedule: sch,
			}
		} else {
			ks.events <- sch
		}
	case kafka.AssignedPartitions:
		err := ks.assign(evt)
		// TODO: sendError(err)
		if err != nil {
			ks.events <- store.Error{
				Event: Error{
					error:     fmt.Errorf("cannot assign partitions %v: %w", evt, err),
					timestamp: time.Now().Unix(),
				},
			}
		}
	case kafka.RevokedPartitions:
		err := ks.revoke(evt)
		// TODO: sendError(err)
		if err != nil {
			ks.events <- store.Error{
				Event: Error{
					error:     fmt.Errorf("cannot revoke partitions %v: %w", evt, err),
					timestamp: time.Now().Unix(),
				},
			}
		}
	case kafka.Error:
		// TODO: sendError(err)
		ks.events <- store.Error{
			Event: Error{
				error:     fmt.Errorf("received kafka error: %w", evt),
				timestamp: time.Now().Unix(),
			},
		}
	default:
		log.Printf("Ignored: %+v", e)
	}
}

func (ks *Store) Events() chan store.Event {
	if ks.events != nil {
		return ks.events
	}

	ks.events = make(chan store.Event, eventsChanBuffer)
	resetTicks := ks.resetTicker.ticks()

	go func() {
		defer log.Printf("kafka store event loop exited ...")
		for {
			select {
			case <-ks.stopChan:
				ks.resetTicker.close()
				return
			case <-resetTicks:
				err := ks.reset()
				if err != nil {
					ks.events <- store.Error{
						Event: Error{
							error:     err,
							timestamp: time.Now().Unix(),
						},
					}
				}
			default:
				ks.processMessage()
			}
		}
	}()

	return ks.events
}
