package kafka

// resetTicker will reset the scheduler to resynch al schedules for the current day
import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

type resetTicker struct {
	stopChan  chan bool
	resetChan chan bool
}

func newResetTicker() resetTicker {
	return resetTicker{
		stopChan:  make(chan bool),
		resetChan: make(chan bool),
	}
}

func (r resetTicker) start() {
	startOfNextDay := func() time.Time {
		day := time.Now().AddDate(0, 0, 1)
		return time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, day.Location())
	}

	ticker := time.NewTimer(time.Until(startOfNextDay()))
	defer ticker.Stop()

	go func() {
		defer log.Println("closing reset ticker ...")
		for {
			ticker.Reset(time.Until(startOfNextDay()))
			select {
			case <-r.stopChan:
				return
			case t := <-ticker.C:
				fmt.Println("Tick at", t)
				r.resetChan <- true
			}
		}
	}()
}

func (r resetTicker) close() {
	r.stopChan <- true
}

func (r resetTicker) ticks() chan bool {
	return r.resetChan
}
