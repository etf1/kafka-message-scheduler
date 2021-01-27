package retry

import "time"

var (
	// DefaultMaxAttempt defines a default maximum retry attempt value.
	DefaultMaxAttempt = 50

	// DefaultStrategy defines a default strategy to be used in configuration
	// if none specified.
	DefaultStrategy = &Backoff{
		Min:    5 * time.Second,
		Max:    1 * time.Hour,
		Factor: 2,
	}
)

// ConfigOption is the type for custom config options
type ConfigOption func(*Config)

// Config provides configuration for retrying a message.
type Config struct {
	MaxAttempt int
	Strategy   Strategy
}

// NewConfig allows to create a new retry configuration object.
func NewConfig(options ...ConfigOption) *Config {
	config := &Config{
		MaxAttempt: DefaultMaxAttempt,
		Strategy:   DefaultStrategy,
	}

	config.apply(options)

	return config
}

func (c *Config) apply(options []ConfigOption) {
	for _, option := range options {
		option(c)
	}
}

// WithMaxAttempt allows to specify a maximum retry attempt value after which
// the message will not be retried anymore.
func WithMaxAttempt(maxAttempt int) ConfigOption {
	return func(c *Config) {
		c.MaxAttempt = maxAttempt
	}
}

// WithStrategy allows to specify a strategy to be used.
func WithStrategy(strategy Strategy) ConfigOption {
	return func(c *Config) {
		c.Strategy = strategy
	}
}
