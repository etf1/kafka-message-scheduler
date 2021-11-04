package instrument_test

import (
	"testing"

	"github.com/etf1/kafka-message-scheduler/instrument"
	"github.com/etf1/kafka-message-scheduler/schedule"
	"github.com/etf1/kafka-message-scheduler/schedule/kafka"
)

// a dummy collector which appends 1 character eacy time a before/after is called
type dummyCollector struct {
	before string
	after  string
	inc    string
	result string
}

func newDummyCollector(before, after, inc string) *dummyCollector {
	return &dummyCollector{
		before: before,
		after:  after,
		inc:    inc,
	}
}

func (dc *dummyCollector) Before(event instrument.EventType, _ schedule.Schedule) {
	dc.result += dc.before
}

func (dc *dummyCollector) After(event instrument.EventType, _ schedule.Schedule, err error) {
	dc.result += dc.after
}

func (dc *dummyCollector) Inc(event instrument.EventType) {
	dc.result += dc.inc
}

// Rule #1: all collector should be called in sequence as passed in argument
func TestMultiCollector(t *testing.T) {
	dc := newDummyCollector("a", "b", "c")

	kafkaSchedule := &kafka.Schedule{}

	mc := instrument.NewMultiCollector(dc)
	mc.Before(instrument.ProducedSchedule, kafkaSchedule)
	mc.After(instrument.ProducedSchedule, kafkaSchedule, nil)
	mc.Inc(instrument.ProducedSchedule)
	if dc.result != "abc" {
		t.Errorf("unexpected result: %q", dc.result)
	}

	dc = newDummyCollector("a", "b", "c")
	mc = instrument.NewMultiCollector(dc, dc)
	mc.Before(instrument.ProducedSchedule, kafkaSchedule)
	mc.After(instrument.ProducedSchedule, kafkaSchedule, nil)
	mc.Inc(instrument.ProducedSchedule)
	if dc.result != "aabbcc" {
		t.Errorf("unexpected result: %q", dc.result)
	}

	dc = newDummyCollector("b", "a", "c")
	mc = instrument.NewMultiCollector(dc, dc)
	mc.Before(instrument.ProducedSchedule, kafkaSchedule)
	mc.After(instrument.ProducedSchedule, kafkaSchedule, nil)
	mc.Inc(instrument.ProducedSchedule)
	if dc.result != "bbaacc" {
		t.Errorf("unexpected result: %q", dc.result)
	}
}
