package main

import (
	"os"
	"os/signal"
	"syscall"

	graylog "github.com/gemnasium/logrus-graylog-hook"
	log "github.com/sirupsen/logrus"

	"github.com/etf1/kafka-message-scheduler/config"
	runner "github.com/etf1/kafka-message-scheduler/runner/kafka"
)

var (
	version = "undefined"
	app     = "kafka-message-scheduler"
)

func main() {
	initLog()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

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

func initLog() {
	log.SetOutput(os.Stdout)
	log.SetLevel(config.LogLevel())
	formatter := &log.TextFormatter{
		FullTimestamp: true,
	}
	log.SetFormatter(formatter)

	if graylogServer := config.GraylogServer(); graylogServer != "" {
		hook := graylog.NewGraylogHook(graylogServer, map[string]interface{}{"app": app, "version": version, "facility": app})
		defer hook.Flush()

		log.AddHook(hook)
	}
}
