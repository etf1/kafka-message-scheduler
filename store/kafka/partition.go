package kafka

import (
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/etf1/kafka-message-scheduler/schedule"
	kafka_schedule "github.com/etf1/kafka-message-scheduler/schedule/kafka"
)

// How kafka partition assignment works in the confluent go client
// with the option 'go.application.rebalance.enable'=true :

// when a consumer starts it will receive the events 'kafka.AssignedPartitions'
// with a list of tuple 'TopicPartition'.
// This list can be empty if there are more consumers in the consumer group
// than partitions in the topic.

// When a new consumer is created it will receive only an event 'kafka.AssignedPartitions',
// but other consumers will receive the following sequence of events:
// 1-kafka.RevokedPartitions: which can be empty if no partitions were assigned
// 2-kafka.AssignedPartitions: which can be empty if no partitions are assigned

// Once a consumer starts, it will receive always the RevokedPartitions and then AssignedPartitions.

// example:
// If we receive the following events from kafka

// revoked partition = [0, 1]
// assigned partition = [1, 2]

// we should send a DeleteSchedules event for partition 0
// and reset offset of the partition to 0 for partition 2
// partition 1 was already in our scope so no offset reset

func (ks *Store) reset() error {
	log.Printf("reset currently assigned=%v", ks.assigned.Partitions)

	for i := range ks.assigned.Partitions {
		tp := ks.assigned.Partitions[i]
		ks.assigned.Partitions[i].Offset = 0
		log.Printf("resetting offset to 0 for %+v", tp)
	}
	log.Printf("calling assign %v with %v", ks.Consumer, ks.assigned.Partitions)
	return ks.Assign(ks.assigned.Partitions)
}

func (ks *Store) revoke(revoked kafka.RevokedPartitions) error {
	ks.revoked = revoked
	log.Printf("revoke paritions=%v with current assigned=%v", ks.revoked.Partitions, ks.assigned.Partitions)
	return ks.Unassign()
}

func (ks *Store) assign(assigned kafka.AssignedPartitions) error {
	ks.assigned = assigned

	log.Printf("assign partitions=%v with current revoked=%v", ks.assigned.Partitions, ks.revoked.Partitions)

	// find deleted partitions : revoked partitions which are not in assigned list
	deleted := minus(ks.revoked.Partitions, ks.assigned.Partitions)

	// find new assigned partitions : assigned partitions which are not revoked
	added := minus(ks.assigned.Partitions, ks.revoked.Partitions)

	// send DeleteSchedules event for deleted paritions
	if len(deleted) != 0 {
		ks.events <- schedule.DeleteSchedules{
			Time: time.Now(),
			DeleteFunc: func(s schedule.Schedule) bool {
				if sch, ok := s.(kafka_schedule.Schedule); ok && isIn(sch.TopicPartition, deleted) {
					return true
				}
				return false
			},
		}
	}

	// for all newly assigned partitions process all from the beginning (reset offset 0)
	if len(ks.assigned.Partitions) != 0 {
		for i := range ks.assigned.Partitions {
			tp := ks.assigned.Partitions[i]
			// reset offset=0 for all newly assigned partitions
			// partitions not new keep the existing offset
			if isIn(tp, added) {
				ks.assigned.Partitions[i].Offset = 0
				log.Printf("resetting offset to 0 for %+v", tp)
			}
		}
		log.Printf("calling assign %v with %v", ks.Consumer, ks.assigned.Partitions)
		return ks.Assign(ks.assigned.Partitions)
	}

	return nil
}
