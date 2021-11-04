package otel

import (
	"context"
	"sync"

	"github.com/etf1/kafka-message-scheduler/instrument"
	"github.com/etf1/kafka-message-scheduler/instrument/otel/internal"
	"github.com/etf1/kafka-message-scheduler/schedule"
	"github.com/etf1/kafka-message-scheduler/schedule/kafka"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"go.opentelemetry.io/contrib"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// Collector is the OpenTelemetry instrumentation collector.
type Collector struct {
	ctx        context.Context
	tracer     oteltrace.Tracer
	propagator propagation.TextMapPropagator
	spans      *sync.Map
}

// NewCollector instanciates a new Collector.
func NewCollector(opts ...Option) *Collector {
	cfg := &config{
		tracerProvider: otel.GetTracerProvider(),
		propagator:     otel.GetTextMapPropagator(),
		tracerName:     tracerName,
	}

	for _, o := range opts {
		o.apply(cfg)
	}

	return &Collector{
		ctx: context.Background(),
		tracer: cfg.tracerProvider.Tracer(
			cfg.tracerName,
			oteltrace.WithInstrumentationVersion(contrib.SemVersion()),
		),
		propagator: cfg.propagator,
		spans:      &sync.Map{},
	}
}

func (c *Collector) attrsByOperationAndMessage(operation internal.Operation, msg *confluent.Message) []attribute.KeyValue {
	attributes := []attribute.KeyValue{
		internal.KafkaSystemKey(),
		internal.KafkaOperation(operation),
		semconv.MessagingDestinationKindTopic,
	}

	if msg != nil {
		attributes = append(
			attributes,
			internal.KafkaMessageKey(string(msg.Key)),
			semconv.MessagingKafkaPartitionKey.Int(int(msg.TopicPartition.Partition)),
		)
		attributes = append(attributes, internal.KafkaMessageHeaders(msg.Headers)...)

		if topic := msg.TopicPartition.Topic; topic != nil {
			attributes = append(attributes, internal.KafkaDestinationTopic(*topic))
		}
	}

	return attributes
}

func (c *Collector) startSpan(operationName internal.Operation, msg *confluent.Message) oteltrace.Span {
	opts := []oteltrace.SpanStartOption{
		oteltrace.WithSpanKind(oteltrace.SpanKindConsumer),
	}

	carrier := NewMessageCarrier(msg)
	ctx := c.propagator.Extract(c.ctx, carrier)

	ctx, span := c.tracer.Start(ctx, string(operationName), opts...)

	c.propagator.Inject(ctx, carrier)

	span.SetAttributes(c.attrsByOperationAndMessage(operationName, msg)...)

	return span
}

// Before is triggered before an event occurred.
func (c *Collector) Before(event instrument.EventType, sch schedule.Schedule) {
	if sch == nil {
		return
	}

	var kafkaSchedule = castToKafkaSchedule(sch)
	if kafkaSchedule.Message == nil {
		return
	}

	var operation internal.Operation

	switch event {
	case instrument.DeletedSchedule:
		operation = internal.OperationDeleted

	case instrument.InvalidSchedule:
		operation = internal.OperationInvalid

	case instrument.PlannedSchedule:
		operation = internal.OperationPlanned

	case instrument.ProducedSchedule:
		operation = internal.OperationProduced

	case instrument.DeleteSchedules, instrument.MissedSchedule, instrument.Other, instrument.TriggeredSchedule:
		return
	}

	span := c.startSpan(operation, kafkaSchedule.Message)

	c.spans.Store(kafkaSchedule.Message, span)
}

// After is triggered before an event occurred.
func (c *Collector) After(event instrument.EventType, sch schedule.Schedule, err error) {
	if sch == nil {
		return
	}

	var kafkaSchedule = castToKafkaSchedule(sch)
	if kafkaSchedule.Message == nil {
		return
	}

	var count = 0
	c.spans.Range(func(key, value interface{}) bool {
		count++
		return true
	})

	if value, ok := c.spans.LoadAndDelete(kafkaSchedule.Message); ok {
		span := value.(oteltrace.Span)
		endSpan(span, err)
	}
}

func (c *Collector) Inc(event instrument.EventType) {
	// Not implemented.
}

func castToKafkaSchedule(sch schedule.Schedule) (kafkaSchedule kafka.Schedule) {
	switch v := sch.(type) {
	case schedule.DeletedSchedule:
		kafkaSchedule = v.Schedule.(kafka.Schedule)

	case schedule.InvalidSchedule:
		kafkaSchedule = v.Schedule.(kafka.Schedule)

	case schedule.MissedSchedule:
		kafkaSchedule = v.Schedule.(kafka.Schedule)

	case kafka.Schedule:
		kafkaSchedule = v
	}

	return
}
