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
	exitedChan    chan bool
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

	consumer, err := kafka.NewConsumer(&finalCfg)
	if err != nil {
		return nil, fmt.Errorf("cannot create kafka consumer for the store: %w", err)
	}

	resetTicker := newResetTicker()

	s := Store{
		Consumer:      consumer,
		stopChan:      make(chan bool),
		exitedChan:    make(chan bool),
		resetTicker:   resetTicker,
		pollTimeoutMs: 10000,
	}

	err = consumer.SubscribeTopics(topics, s.processRebalance)
	if err != nil {
		return nil, fmt.Errorf("cannot subscribe to topics '%s': %w", topics, err)
	}

	resetTicker.start()

	return &s, nil
}

// TODO remove return error
func (ks *Store) Close() error {
	defer log.Println("kafka store closed")
	log.Println("kafka store closing ...")

	// If ks.events is nil means that the go routine for events has never been launched
	// so no need to signal in the stopChan.
	if ks.events != nil {
		ks.stopChan <- true
		// Wait for the poller to exit before we close the consumer.  If we don't, the poller may
		// segfault if it has not exited yet and still tries to poll the destroyed queue.
		<-ks.exitedChan
	}

	// Close the consumer before closing the events channel, just in case the rebalance callback
	// is still called with revoked partitions.
	closeErr := ks.Consumer.Close()

	if ks.events != nil {
		close(ks.events)
	}
	return closeErr
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
		err := schedule.CheckSchedule(sch)
		if err != nil {
			ks.events <- schedule.InvalidSchedule{
				Schedule: sch,
				Error:    err,
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

func (ks *Store) processRebalance(consumer *kafka.Consumer, e kafka.Event) error {
	switch evt := e.(type) {
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
	default:
		log.Printf("Ignored rebalance-related event: %+v", e)
	}
	return nil
}

func (ks *Store) Events() chan store.Event {
	if ks.events != nil {
		return ks.events
	}

	ks.events = make(chan store.Event, eventsChanBuffer)
	resetTicks := ks.resetTicker.ticks()

	go func() {
		defer func() { close(ks.exitedChan) }()
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
