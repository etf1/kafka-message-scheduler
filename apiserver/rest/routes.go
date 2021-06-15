package rest

import (
	"encoding/json"
	"net"
	"net/http"
	"os"

	"github.com/etf1/kafka-message-scheduler/config"
	"github.com/etf1/kafka-message-scheduler/schedule"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

func (s *Server) initRouter() *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/liveness", s.isAlive).Methods(http.MethodGet)
	router.HandleFunc("/info", s.info).Methods(http.MethodGet)
	router.HandleFunc("/schedules", s.schedules).Methods(http.MethodGet)
	return router
}

func (s *Server) isAlive(w http.ResponseWriter, r *http.Request) {
	if !s.IsAlive() {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) schedules(w http.ResponseWriter, r *http.Request) {
	list := s.Timers.GetAll()

	if len(list) == 0 {
		err := respondWithJSON(w, http.StatusOK, []schedule.Schedule{})
		if err != nil {
			log.Errorf("cannot write json payload: %v", err)
		}
		return
	}

	err := respondWithJSON(w, http.StatusOK, list)
	if err != nil {
		log.Errorf("cannot write json payload: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (s *Server) info(w http.ResponseWriter, r *http.Request) {
	host, err := os.Hostname()
	if err != nil {
		log.Errorf("cannot get hostname: %v", host)
	}

	ips := make([]net.IP, 0)

	ifaces, err := net.Interfaces()
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for _, i := range ifaces {
		addrs, err2 := i.Addrs()
		if err != nil {
			log.Errorf("cannot get addresses from interface %v: %v", i, err2)
			break
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			ips = append(ips, ip)
		}
	}

	type kafka struct {
		BootstrapServers string   `json:"bootstrap_servers"`
		Topics           []string `json:"topics"`
		HistoryTopic     string   `json:"history_topic"`
	}
	type info struct {
		Host          string   `json:"hostname"`
		Address       []net.IP `json:"address"`
		ServerAddress string   `json:"server_address"`
		kafka         `json:"kafka"`
	}

	result := info{
		Host:          host,
		Address:       ips,
		ServerAddress: config.ServerAddr(),
		kafka: kafka{
			BootstrapServers: config.BootstrapServers(),
			Topics:           config.SchedulesTopics(),
			HistoryTopic:     config.HistoryTopic(),
		},
	}

	err = respondWithJSON(w, http.StatusOK, result)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	err := respondWithJSON(w, code, map[string]string{"error": message})
	if err != nil {
		log.Errorf("cannot respond with error: %v", err)
	}
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) error {
	if payload != nil {
		response, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)

		_, err = w.Write(response)
		if err != nil {
			log.Errorf("cannot write response: %v", err)
		}
	} else {
		w.WriteHeader(code)
	}

	return nil
}
