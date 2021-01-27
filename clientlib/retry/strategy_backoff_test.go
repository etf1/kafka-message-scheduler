package retry

import (
	"testing"
	"time"
)

func TestBackOff(t *testing.T) {
	testCases := []struct {
		caseName         string
		backoff          *Backoff
		attempt          int
		expectedDuration time.Duration
	}{
		{
			caseName:         "Test case for factor 2, attempt #1",
			backoff:          &Backoff{Min: 10 * time.Second, Max: 1 * time.Hour, Factor: 2},
			attempt:          1,
			expectedDuration: 20 * time.Second,
		},
		{
			caseName:         "Test case for factor 2, attempt #2",
			backoff:          &Backoff{Min: 10 * time.Second, Max: 1 * time.Hour, Factor: 2},
			attempt:          2,
			expectedDuration: 40 * time.Second,
		},
		{
			caseName:         "Test case for factor 2, attempt #3",
			backoff:          &Backoff{Min: 10 * time.Second, Max: 1 * time.Hour, Factor: 2},
			attempt:          3,
			expectedDuration: 80 * time.Second,
		},
		{
			caseName:         "Test case for factor 2, attempt #4",
			backoff:          &Backoff{Min: 10 * time.Second, Max: 1 * time.Hour, Factor: 2},
			attempt:          4,
			expectedDuration: 160 * time.Second,
		},
		{
			caseName:         "Test case for factor 2, attempt #50 (more than 1 hour), should return 1 hour (max value)",
			backoff:          &Backoff{Min: 10 * time.Second, Max: 1 * time.Hour, Factor: 2},
			attempt:          20,
			expectedDuration: 1 * time.Hour,
		},
		{
			caseName:         "Test case for factor 1, attempt #1",
			backoff:          &Backoff{Min: 10 * time.Second, Max: 1 * time.Hour, Factor: 1},
			attempt:          1,
			expectedDuration: 10 * time.Second,
		},
		{
			caseName:         "Test case for factor 1, attempt #2",
			backoff:          &Backoff{Min: 10 * time.Second, Max: 1 * time.Hour, Factor: 1},
			attempt:          2,
			expectedDuration: 10 * time.Second,
		},
		{
			caseName:         "Test case for factor 1.5, attempt #1",
			backoff:          &Backoff{Min: 10 * time.Second, Max: 1 * time.Hour, Factor: 1.5},
			attempt:          1,
			expectedDuration: 15 * time.Second,
		},
		{
			caseName:         "Test case for factor 1.5, attempt #2",
			backoff:          &Backoff{Min: 10 * time.Second, Max: 1 * time.Hour, Factor: 1.5},
			attempt:          2,
			expectedDuration: 22500 * time.Millisecond,
		},
		{
			caseName:         "Test case for factor 1, attempt #2",
			backoff:          &Backoff{Min: 10 * time.Second, Max: 1 * time.Hour, Factor: 1},
			attempt:          2,
			expectedDuration: 10 * time.Second,
		},
		{
			caseName:         "Test case for factor 3, attempt #1",
			backoff:          &Backoff{Min: 10 * time.Second, Max: 1 * time.Hour, Factor: 3},
			attempt:          1,
			expectedDuration: 30 * time.Second,
		},
		{
			caseName:         "Test case for factor 3, attempt #2",
			backoff:          &Backoff{Min: 10 * time.Second, Max: 1 * time.Hour, Factor: 3},
			attempt:          2,
			expectedDuration: 90 * time.Second,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.caseName, func(t *testing.T) {
			duration := testCase.backoff.DurationForAttempt(testCase.attempt)

			if testCase.expectedDuration != duration {
				t.Errorf("Wrong duration given (%v) for timestamp (%d)", duration, testCase.attempt)
			}
		})
	}
}
