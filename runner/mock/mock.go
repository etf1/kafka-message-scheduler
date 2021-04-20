package mock

// Kafka runner for the scheduler

import (
	"time"

	hmapcoll "github.com/etf1/kafka-message-scheduler/internal/collector/hmap"
	"github.com/etf1/kafka-message-scheduler/internal/test"
	"github.com/etf1/kafka-message-scheduler/schedule/kafka"
	log "github.com/sirupsen/logrus"

	"github.com/etf1/kafka-message-scheduler/apiserver/rest"
	"github.com/etf1/kafka-message-scheduler/config"
	"github.com/etf1/kafka-message-scheduler/internal/store/hmap"
	"github.com/etf1/kafka-message-scheduler/scheduler"
)

var (
	Schedules = []kafka.Schedule{
		{Message: test.FullMessage("scheduler", "schedule-1", "value 1", time.Now().Add(1*time.Hour).Unix(), "topic", "1")},
		{Message: test.FullMessage("scheduler", "schedule-2", "value 2", time.Now().Add(1*time.Hour).Unix(), "topic", "2")},
		{Message: test.FullMessage("scheduler", "schedule-3", "value 3", time.Now().Add(1*time.Hour).Unix(), "topic", "3")},
	}
)

type Runner struct {
	stopChan chan bool
}

func NewRunner() *Runner {
	return &Runner{
		stopChan: make(chan bool),
	}
}

func (r Runner) Close() error {
	r.stopChan <- true
	return nil
}

func (r *Runner) Start() error {
	store := hmap.New()

	for _, m := range Schedules {
		store.Add(m)
	}

	handler := NewHandler()

	sch := scheduler.New(store, hmapcoll.New())
	sch.Start(scheduler.StartOfToday())

	srv := rest.New(&sch)
	srv.Start(config.APIServerAddr())

	events := sch.Events()

loop:
	for {
		select {
		case event, open := <-events:
			if !open {
				break loop
			}
			handler.Handle(event)
		case <-r.stopChan:
			err := srv.Stop()
			if err != nil {
				log.Errorf("error when stopping api server: %v", err)
			}
			sch.Close()
		}
	}

	return nil
}
