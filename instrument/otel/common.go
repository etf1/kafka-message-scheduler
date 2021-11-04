package otel

import (
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const (
	// tracerName is the technical name of the tracer.
	tracerName = "github.com/etf1/kafka-message-scheduler/instrument/otel"
)

func endSpan(s oteltrace.Span, err error) {
	if err != nil {
		s.SetStatus(codes.Error, err.Error())
	}
	s.End()
}
