package retry

import (
	"fmt"
	"testing"
	"time"
)

func TestNewConfig(t *testing.T) {
	config := NewConfig()

	checkValue(t, fmt.Sprintf("%T", config), "*retry.Config")
	checkValue(t, config.MaxAttempt, DefaultMaxAttempt)
	checkValue(t, config.Strategy, DefaultStrategy)
}

func TestNewConfig_option_with_max_attempt(t *testing.T) {
	config := NewConfig(WithMaxAttempt(10))

	checkValue(t, fmt.Sprintf("%T", config), "*retry.Config")
	checkValue(t, config.MaxAttempt, 10)
	checkValue(t, config.Strategy, DefaultStrategy)
}

func TestNewConfig_option_with_strategy(t *testing.T) {
	customStrategy := &Backoff{
		Factor: 2.5,
		Min:    5 * time.Second,
		Max:    30 * time.Second,
	}

	config := NewConfig(WithStrategy(customStrategy))

	checkValue(t, fmt.Sprintf("%T", config), "*retry.Config")
	checkValue(t, config.MaxAttempt, DefaultMaxAttempt)
	checkValue(t, config.Strategy, customStrategy)
}

func TestNewConfig_option_all(t *testing.T) {
	maxAttempt := 10
	customStrategy := &Backoff{
		Factor: 2.5,
		Min:    5 * time.Second,
		Max:    30 * time.Second,
	}

	config := NewConfig(WithMaxAttempt(maxAttempt), WithStrategy(customStrategy))

	checkValue(t, fmt.Sprintf("%T", config), "*retry.Config")
	checkValue(t, config.MaxAttempt, maxAttempt)
	checkValue(t, config.Strategy, customStrategy)
}

func checkValue(t *testing.T, current, expected interface{}) {
	if value := current; value != expected {
		t.Errorf("wrong value for Topic property, have '%q', expected: '%q'", current, expected)
	}
}
