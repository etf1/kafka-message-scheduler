package kafka

import (
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

func isEquals(a, b confluent.TopicPartition) bool {
	return *a.Topic == *b.Topic && a.Partition == b.Partition
}

// returns TopicPartitions which are in arr1 and not in arr2
func minus(arr1, arr2 confluent.TopicPartitions) confluent.TopicPartitions {
	result := confluent.TopicPartitions{}
	var keep bool
	for _, a := range arr1 {
		keep = true
		for _, b := range arr2 {
			if isEquals(a, b) {
				keep = false
			}
		}
		if keep {
			result = append(result, a)
		}
	}
	return result
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
