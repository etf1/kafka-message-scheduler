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
	assigned, err := ks.Assignment()
	if err != nil {
		return err
	}
	log.Printf("reset currently assigned to 0 offset=%v", assigned)

	for i := range assigned {
		assigned[i].Offset = 0
	}
	// At this time, we have to loop and seek each partition individually.  When
	//
	//     https://github.com/confluentinc/confluent-kafka-go/issues/902
	//     Add SeekPartitions wrapper for rd_kafka_seek_partitions function to support seeking
	//     multiple partitions at a time
	//
	// is addressed, we can just pass the entire assigned variable to the proposed SeekPartitions
	// function instead of having this loop here.
	for i := range assigned {
		log.Printf("calling Seek %v with %v", ks.Consumer, assigned[i])
		err := ks.Seek(assigned[i], 0)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ks *Store) revoke(revoked kafka.RevokedPartitions) error {
	log.Printf("revoke partitions=%v due to assignment lost=%v", revoked.Partitions, ks.AssignmentLost())

	// send DeleteSchedules event for deleted partitions
	ks.events <- schedule.DeleteSchedules{
		Time: time.Now(),
		DeleteFunc: func(s schedule.Schedule) bool {
			if sch, ok := s.(kafka_schedule.Schedule); ok && isIn(sch.TopicPartition, revoked.Partitions) {
				return true
			}
			return false
		},
	}

	log.Printf("calling IncrementalUnassign %v with %v", ks.Consumer, revoked.Partitions)
	return ks.IncrementalUnassign(revoked.Partitions)
}

func (ks *Store) assign(assigned kafka.AssignedPartitions) error {
	log.Printf("incremental assign partitions and reset offset to 0 for=%v", assigned.Partitions)

	// for all newly assigned partitions process all from the beginning (reset offset 0)
	for i := range assigned.Partitions {
		assigned.Partitions[i].Offset = 0
	}

	log.Printf("calling IncrementalAssign %v with %v", ks.Consumer, assigned.Partitions)
	return ks.IncrementalAssign(assigned.Partitions)
}
