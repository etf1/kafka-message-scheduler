package instrument

// Allow to wrap multiple collectors as one collector
import (
	"github.com/etf1/kafka-message-scheduler/schedule"
)

// MultiCollector allows you to compose a list of collector
type MultiCollector struct {
	collectors []Collector
}

// NewMultiCollector is a constructor for multi collector
func NewMultiCollector(collectors ...Collector) MultiCollector {
	return MultiCollector{
		collectors: collectors,
	}
}

/*
type Closeable interface {
	Close() error
}

func (m MultiCollector) Close() error {
	for i := 0; i < len(m.collectors); i++ {
		if cl, ok := m.collectors[i].(Closeable); ok {
			err := cl.Close()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
*/

// Before calls all Before method of inner collectors
func (m MultiCollector) Before(event EventType, sch schedule.Schedule) {
	for i := 0; i < len(m.collectors); i++ {
		m.collectors[i].Before(event, sch)
	}
}

// After calls all After method of inner collectors
func (m MultiCollector) After(event EventType, sch schedule.Schedule, err error) {
	for i := 0; i < len(m.collectors); i++ {
		m.collectors[i].After(event, sch, err)
	}
}

// Inc calls all Inc method of inner collectors
func (m MultiCollector) Inc(event EventType) {
	for i := 0; i < len(m.collectors); i++ {
		m.collectors[i].Inc(event)
	}
}
