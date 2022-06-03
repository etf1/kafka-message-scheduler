package main

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"

	runner "github.com/etf1/kafka-message-scheduler/runner/kafka"
)

var (
	version = "undefined"
	app     = "kafka-message-scheduler"
)

func main() {
	closePprof := initPprof()
	defer closePprof()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	initLog()

	kafkaRunner := runner.DefaultRunner()

	exitchan := make(chan bool)

	go func() {
		log.Printf("starting scheduler version=%v", version)
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
		case <-exitchan:
			log.Printf("scheduler exited")
			break loop
		}
	}

	log.Printf("exiting ...")
}
