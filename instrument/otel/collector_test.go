package otel

import (
	"context"
	"sync"
	"testing"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/etf1/kafka-message-scheduler/instrument"
	"github.com/etf1/kafka-message-scheduler/schedule"
	"github.com/etf1/kafka-message-scheduler/schedule/kafka"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func TestNewCollector(t *testing.T) {
	// When
	collector := NewCollector()

	// Then
	assert.IsType(t, new(Collector), collector)

	assert.Equal(t, context.Background(), collector.ctx)
	assert.Equal(t, otel.GetTextMapPropagator(), collector.propagator)
	assert.Equal(t, new(sync.Map), collector.spans)
}

func TestNewCollector_WithTracerProvider(t *testing.T) {
	// Given
	tracerProvider := otel.GetTracerProvider()

	// When
	collector := NewCollector(WithTracerProvider(tracerProvider))

	// Then
	assert.IsType(t, new(Collector), collector)

	assert.Equal(t, context.Background(), collector.ctx)
	assert.Equal(t, otel.GetTextMapPropagator(), collector.propagator)
	assert.Equal(t, new(sync.Map), collector.spans)
}

func TestCollector_Before_Invalid(t *testing.T) {
	// Given
	tracerProvider := otel.GetTracerProvider()

	collector := NewCollector(WithTracerProvider(tracerProvider))

	kafkaSchedule := schedule.InvalidSchedule{
		Schedule: kafka.Schedule{
			Message: &confluent.Message{
				Key:   []byte("key"),
				Value: []byte("value"),
			}},
	}

	// When
	collector.Before(instrument.InvalidSchedule, kafkaSchedule)

	// Then
	value, ok := collector.spans.Load(kafkaSchedule.Schedule.(kafka.Schedule).Message)
	span := value.(trace.Span)

	assert.True(t, ok)
	assert.Equal(t, "00000000000000000000000000000000", span.SpanContext().TraceID().String())
	assert.Equal(t, "0000000000000000", span.SpanContext().SpanID().String())
}

func TestCollector_Before_Deleted(t *testing.T) {
	// Given
	tracerProvider := otel.GetTracerProvider()

	collector := NewCollector(WithTracerProvider(tracerProvider))

	kafkaSchedule := schedule.DeletedSchedule{
		Schedule: kafka.Schedule{
			Message: &confluent.Message{
				Key:   []byte("key"),
				Value: []byte("value"),
			}},
	}

	// When
	collector.Before(instrument.DeletedSchedule, kafkaSchedule)

	// Then
	value, ok := collector.spans.Load(kafkaSchedule.Schedule.(kafka.Schedule).Message)
	span := value.(trace.Span)

	assert.True(t, ok)
	assert.Equal(t, "00000000000000000000000000000000", span.SpanContext().TraceID().String())
	assert.Equal(t, "0000000000000000", span.SpanContext().SpanID().String())
}

func TestCollector_Before_Planned(t *testing.T) {
	// Given
	tracerProvider := otel.GetTracerProvider()

	collector := NewCollector(WithTracerProvider(tracerProvider))

	kafkaSchedule := kafka.Schedule{
		Message: &confluent.Message{
			Key:   []byte("key"),
			Value: []byte("value"),
		}}

	// When
	collector.Before(instrument.PlannedSchedule, kafkaSchedule)

	// Then
	value, ok := collector.spans.Load(kafkaSchedule.Message)
	span := value.(trace.Span)

	assert.True(t, ok)
	assert.Equal(t, "00000000000000000000000000000000", span.SpanContext().TraceID().String())
	assert.Equal(t, "0000000000000000", span.SpanContext().SpanID().String())
}

func TestCollector_Before_AnotherAction(t *testing.T) {
	// Given
	tracerProvider := otel.GetTracerProvider()

	collector := NewCollector(WithTracerProvider(tracerProvider))

	kafkaSchedule := kafka.Schedule{
		Message: &confluent.Message{
			Key:   []byte("key"),
			Value: []byte("value"),
		}}

	// When
	collector.Before(instrument.Other, kafkaSchedule)

	// Then
	value, ok := collector.spans.Load(kafkaSchedule)

	assert.Nil(t, value)
	assert.False(t, ok)
}
