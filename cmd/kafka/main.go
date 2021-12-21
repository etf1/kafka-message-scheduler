package main

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"

	runner "github.com/etf1/kafka-message-scheduler/runner/kafka"
)

var (
	Version = "undefined"
	App     = "kafka-message-scheduler"
)

func main() {
	defer initPprof()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	initLog()

	coll, cancel := runner.Collector(App, Version)

	kafkaRunner := runner.NewRunner(
		runner.DefaultConfig(),
		runner.DefaultSince(),
		coll,
	)

	exitchan := make(chan bool)

	go func() {
		log.Printf("starting scheduler version=%v", Version)
		if err := kafkaRunner.Start(); err != nil {
			log.Errorf("failed to start scheduler: %v", err)
		}
		exitchan <- true
	}()

loop:
	for {
		select {
		case <-sigchan:
			kafkaRunner.Close()
			err := cancel()
			if err != nil {
				log.Errorf("cannot close collector: %v", err)
			}
		case <-exitchan:
			log.Printf("scheduler exited")
			break loop
		}
	}

	log.Printf("exiting ...")
}
