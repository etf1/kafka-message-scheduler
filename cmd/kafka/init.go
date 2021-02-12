package main

import (
	"context"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/etf1/kafka-message-scheduler/config"
	graylog "github.com/gemnasium/logrus-graylog-hook"
	log "github.com/sirupsen/logrus"
)

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

func initPprof() func() {
	timeout := 5 * time.Second
	startchan := make(chan os.Signal, 1)
	signal.Notify(startchan, syscall.SIGUSR1)

	stopchan := make(chan os.Signal, 1)
	signal.Notify(stopchan, syscall.SIGUSR2)

	exitchan := make(chan bool)

	var server *http.Server

	shutdown := func() {
		if server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			log.Printf("shutting down pprof server")
			log.Printf("%v", server.Shutdown(ctx))
		}
	}

	closePprof := func() {
		shutdown()
		exitchan <- true
	}

	router := http.NewServeMux()
	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	router.HandleFunc("/debug/pprof/trace", pprof.Trace)

	go func() {
		defer log.Printf("pprof launcher exited")
		for {
			select {
			case <-exitchan:
				return
			case <-stopchan:
				shutdown()
			case <-startchan:
				server = &http.Server{
					Addr:    ":6060",
					Handler: router,
				}
				go func() {
					log.Printf("starting http pprof server")
					log.Println(server.ListenAndServe())
					log.Printf("http server pprof shutted down")
					server = nil
				}()
			}
		}
	}()

	return closePprof
}
