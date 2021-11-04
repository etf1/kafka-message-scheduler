package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"

	"github.com/etf1/kafka-message-scheduler/config"
	"github.com/etf1/kafka-message-scheduler/instrument"
	"github.com/etf1/kafka-message-scheduler/instrument/otel"
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

	ctx := context.Background()

	tracerProvider := otel.GetTracerProvider(ctx, config.OpenTelemetryCollectorEndpoint(), App, Version)

	kafkaRunner := runner.NewRunner(
		runner.DefaultConfig(),
		runner.DefaultSince(),
		instrument.NewMultiCollector(
			runner.PrometheusCollector(),
			runner.OtelCollector(tracerProvider),
		),
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
			_ = tracerProvider.Shutdown(ctx)
		case <-exitchan:
			log.Printf("scheduler exited")
			break loop
		}
	}

	log.Printf("exiting ...")
}
