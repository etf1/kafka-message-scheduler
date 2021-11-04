package otel

import (
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type config struct {
	tracerName     string
	tracerProvider oteltrace.TracerProvider
	propagator     propagation.TextMapPropagator
}

// Option is used to configure the client.
type Option interface {
	apply(*config)
}

type optionFunc func(*config)

func (o optionFunc) apply(c *config) {
	o(c)
}

// WithTracerName specifies a specific name for the current tracer.
func WithTracerName(name string) Option {
	return optionFunc(func(cfg *config) {
		cfg.tracerName = name
	})
}

// WithTracerProvider specifies a tracer provider to use for creating a tracer.
// If none is specified, the global provider is used.
func WithTracerProvider(provider oteltrace.TracerProvider) Option {
	return optionFunc(func(cfg *config) {
		if provider != nil {
			cfg.tracerProvider = provider
		}
	})
}

// WithPropagator specifies a custom TextMapPropagator.
func WithPropagator(propagator propagation.TextMapPropagator) Option {
	return optionFunc(func(cfg *config) {
		cfg.propagator = propagator
	})
}
