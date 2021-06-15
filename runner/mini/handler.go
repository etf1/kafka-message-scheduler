package mini

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/etf1/kafka-message-scheduler/internal/store/hmap"
	"github.com/etf1/kafka-message-scheduler/schedule"
	"github.com/etf1/kafka-message-scheduler/scheduler"
)

type Sysout struct {
	store hmap.Hmap
}

func NewHandler(store hmap.Hmap) Sysout {
	return Sysout{store}
}

// Print and reschedule all triggered schedules, so there is always 3 schedules planned
func (s Sysout) Handle(event scheduler.Event) {
	fmt.Printf("handler received event: %+v\n", event)
	sch, ok := event.(schedule.Schedule)
	if ok {
		id := strings.ReplaceAll(sch.ID(), "schedule-", "")
		i, err := strconv.Atoi(id)
		if err == nil {
			s.store.Add(GetSchedule(i))
		}
	}
}
