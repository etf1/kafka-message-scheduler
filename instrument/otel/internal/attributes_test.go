package internal

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
)

func TestAttributes_KafkaMessageHeaders(t *testing.T) {
	testCases := []struct {
		name     string
		msg      []kafka.Header
		expected []attribute.KeyValue
	}{
		{
			name:     "No header",
			msg:      nil,
			expected: nil,
		},
		{
			name: "Single header",
			msg: []kafka.Header{
				{Key: "header-1", Value: []byte("value-1")},
			},
			expected: []attribute.KeyValue{
				{Key: "messaging.headers.header-1", Value: attribute.StringValue("value-1")},
			},
		},
		{
			name: "Multiple headers",
			msg: []kafka.Header{
				{Key: "header-1", Value: []byte("value-1")},
				{Key: "header-2", Value: []byte("value-2")},
			},
			expected: []attribute.KeyValue{
				{Key: "messaging.headers.header-1", Value: attribute.StringValue("value-1")},
				{Key: "messaging.headers.header-2", Value: attribute.StringValue("value-2")},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			attributes := KafkaMessageHeaders(testCase.msg)
			assert.Equal(t, testCase.expected, attributes)
		})
	}
}
