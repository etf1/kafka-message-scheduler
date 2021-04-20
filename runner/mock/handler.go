package mock

import (
	"fmt"

	"github.com/etf1/kafka-message-scheduler/scheduler"
)

type Sysout struct{}

func NewHandler() Sysout {
	return Sysout{}
}

func (s Sysout) Handle(event scheduler.Event) {
	fmt.Printf("mock handler received event: %+v", event)
}
