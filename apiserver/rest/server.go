package rest

import (
	"context"
	"net/http"
	"time"

	"github.com/etf1/kafka-message-scheduler/scheduler"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

var (
	defaultShutdownTimeout = 5 * time.Second
)

type Server struct {
	*http.Server
	*scheduler.Scheduler
}

func New(sch *scheduler.Scheduler) Server {
	s := Server{}

	s.Server = &http.Server{
		Handler: s.initRouter(),
	}

	s.Scheduler = sch

	return s
}

func (s *Server) Router() *mux.Router {
	return s.Handler.(*mux.Router)
}

func (s *Server) Start(addr string) {
	go func() {
		log.Printf("starting rest api server on %s", addr)
		defer log.Printf("rest api server stopped")

		s.Addr = addr

		if err := s.ListenAndServe(); err != nil {
			log.Error(err)
		}
	}()
}

func (s *Server) Stop() error {
	defer log.Printf("rest api server shut down")
	log.Printf("shutting down rest api server")

	ctx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer cancel()

	if err := s.Shutdown(ctx); err != nil {
		return err
	}

	return nil
}
