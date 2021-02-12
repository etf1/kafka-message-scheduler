package kafka

// Kafka runner for the scheduler

import (
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/etf1/kafka-message-scheduler/apiserver/rest"
	"github.com/etf1/kafka-message-scheduler/config"
	"github.com/etf1/kafka-message-scheduler/instrument"
	"github.com/etf1/kafka-message-scheduler/instrument/prometheus"
	"github.com/etf1/kafka-message-scheduler/scheduler"
	"github.com/etf1/kafka-message-scheduler/store/kafka"
)

type Config struct {
	BootstrapServers string
	HistoryTopic     string
	GroupID          string
	SessionTimeout   int
	SchedulesTopics  []string
}

func (c Config) String() string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("boostrap_servers=%v", c.BootstrapServers))
	sb.WriteString(" ")
	sb.WriteString(fmt.Sprintf("groupID=%v", c.GroupID))
	sb.WriteString(" ")
	sb.WriteString(fmt.Sprintf("sessionTimeout=%v", c.SessionTimeout))
	sb.WriteString(" ")
	sb.WriteString(fmt.Sprintf("schedulesTopics=%v", c.SchedulesTopics))

	return sb.String()
}

type Runner struct {
	config    Config
	since     time.Time
	stopChan  chan bool
	collector instrument.Collector
}

func DefaultCollector() prometheus.Collector {
	return prometheus.NewCollector(config.MetricsHTTPAddr())
}

func DefaultConfig() Config {
	return Config{
		BootstrapServers: config.BootstrapServers(),
		GroupID:          config.GroupID(),
		SchedulesTopics:  config.SchedulesTopics(),
		SessionTimeout:   config.SessionTimeout(),
		HistoryTopic:     config.HistoryTopic(),
	}
}

func DefaultSince() time.Time {
	return scheduler.StartOfDayAsTime(config.SinceDelta())
}

func DefaultRunnerParams() (Config, time.Time, instrument.Collector) {
	return DefaultConfig(), DefaultSince(), DefaultCollector()
}

func DefaultRunner() *Runner {
	return NewRunner(DefaultRunnerParams())
}

func NewRunner(c Config, since time.Time, collector instrument.Collector) *Runner {
	return &Runner{
		config:    c,
		since:     since,
		stopChan:  make(chan bool),
		collector: collector,
	}
}

func (r Runner) Close() error {
	r.stopChan <- true
	return nil
}

// used to determine if the colector has close method
type closer interface {
	Close() error
}

func (r *Runner) Start() error {
	if r.config.BootstrapServers == "" {
		return fmt.Errorf("kafka bootstrap servers unset, check variable environment ${BOOTSTRAP_SERVERS}")
	}
	if r.since.After(time.Now()) {
		return fmt.Errorf("since cannot be after current day, check since parameter should be <= 0")
	}

	// If collector contains a Close function, call it
	if c, ok := r.collector.(closer); ok {
		defer c.Close()
	}

	handler, err := NewHandler(r.config.BootstrapServers, r.config.HistoryTopic)
	if err != nil {
		return err
	}
	defer handler.Close()

	log.Printf("config: %v", r.config)
	log.Printf("handler: %v", handler)

	store, err := kafka.NewStore(r.config.BootstrapServers, r.config.SchedulesTopics, r.config.GroupID, r.config.SessionTimeout)
	if err != nil {
		return err
	}
	defer store.Close()

	sch := scheduler.New(store, r.collector)
	sch.Start(r.since)

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
