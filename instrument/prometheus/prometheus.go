package prometheus

import (
	"context"
	"fmt"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/etf1/kafka-message-scheduler/instrument"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	defaultShutdownTimeout = 5 * time.Second
)

type Collector struct {
	eventCounter *prometheus.CounterVec
	srv          *http.Server
}

// NewCollector create a prometheus collector
func NewCollector(port string) Collector {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", promhttp.Handler().ServeHTTP)

	srv := &http.Server{
		Addr:    fmt.Sprintf("127.0.0.1:%s", port),
		Handler: mux,
	}

	go func() {
		log.Printf("prometheus metrics available on 0.0.0.0:%s/metrics", port)
		if err := srv.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				log.Error(err)
			}
		}
	}()

	c := Collector{
		srv: srv,
		eventCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka_scheduler",
				Name:      "event_total",
				Help:      "The number of event triggered by type",
			},
			[]string{"type"},
		),
	}

	prometheus.MustRegister(c.eventCounter)

	return c
}

func (c Collector) Close() error {
	defer log.Printf("prometheus collector closed")

	ctx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer cancel()

	if err := c.srv.Shutdown(ctx); err != nil {
		return err
	}

	return nil
}

func (c Collector) Inc(e instrument.EventType) {
	c.eventCounter.WithLabelValues(string(e)).Inc()
}
