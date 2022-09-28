package kafka

import (
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

func isEquals(a, b confluent.TopicPartition) bool {
	return *a.Topic == *b.Topic && a.Partition == b.Partition
}

// returns true if the item is in the array
func isIn(item confluent.TopicPartition, arr confluent.TopicPartitions) bool {
	for _, tp := range arr {
		if isEquals(item, tp) {
			return true
		}
	}
	return false
}
