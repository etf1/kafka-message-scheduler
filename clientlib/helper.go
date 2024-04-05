package clientlib

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func getHeader(message *kafka.Message, name string) kafka.Header {
	if message == nil {
		return kafka.Header{}
	}

	for _, header := range message.Headers {
		if header.Key == name {
			return header
		}
	}

	return kafka.Header{}
}

func getHeaderValue(msg *kafka.Message, key string) (string, bool) {
	for _, header := range msg.Headers {
		if header.Key == key && len(header.Value) > 0 {
			return string(header.Value), true
		}
	}
	return "", false
}

func updateOrAddHeader(message *kafka.Message, key string, value []byte) {
	for i, header := range message.Headers {
		if header.Key == key {
			message.Headers[i].Value = value
			return
		}
	}

	message.Headers = append(message.Headers, kafka.Header{
		Key:   key,
		Value: value,
	})
}
