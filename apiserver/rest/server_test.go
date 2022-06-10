package rest_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/etf1/kafka-message-scheduler/config"
	"github.com/etf1/kafka-message-scheduler/schedule/simple"
)

// Rule #1: scheduler should have an endpoint for checking it is still alive
func TestServer_isalive(t *testing.T) {
	s, closeFunc := newServer()
	defer closeFunc()

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/liveness", http.NoBody)
	response := executeRequest(s.Router(), req)

	checkResponseCode(t, http.StatusOK, response.Code)
}

// Rule #2: scheduler should have an endpoint for retrieving general information on the scheduler setup
func TestServer_info(t *testing.T) {
	s, closeFunc := newServer()
	defer closeFunc()

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/info", http.NoBody)
	response := executeRequest(s.Router(), req)

	checkResponseCode(t, http.StatusOK, response.Code)

	type info struct {
		Host          string   `json:"hostname"`
		Address       []string `json:"address"`
		ServerAddress string   `json:"server_address"`
		Kafka         struct {
			BootstrapServers string   `json:"bootstrap_servers"`
			Topics           []string `json:"topics"`
			HistoryTopic     string   `json:"history_topic"`
		} `json:"kafka"`
	}

	obj := info{}
	err := json.Unmarshal(response.Body.Bytes(), &obj)
	if err != nil {
		t.Fatalf("unable to unmarshall json body: %v - %s", err, response.Body.Bytes())
	}

	t.Logf("info: %s", response.Body.String())

	if v := obj.ServerAddress; v != config.ServerAddr() {
		t.Errorf("unexpected api server address: %v", v)
	}
	if v := obj.Host; v == "" {
		t.Errorf("unexpected host: %v", v)
	}
	if v := len(obj.Address); v == 0 {
		t.Errorf("unexpected address length: %v", v)
	}
	if v := obj.Kafka.BootstrapServers; v != config.BootstrapServers() {
		t.Errorf("unexpected kafka bootstrap servers: %v", v)
	}
	if v := obj.Kafka.Topics; len(v) > 0 && v[0] != "schedules" {
		t.Errorf("unexpected kafka topics: %v", v)
	}
	if v := obj.Kafka.HistoryTopic; v != "history" {
		t.Errorf("unexpected kafka history topic: %v", v)
	}
}

// scheduler should have an endpoint for retrieving the list of schedules planned to be scheduled in memory

// Rule #3: schedules endpoint should return 200 when no schedules found
func TestServer_schedules_notfound(t *testing.T) {
	s, closeFunc := newServer()
	defer closeFunc()

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/schedules", http.NoBody)
	response := executeRequest(s.Router(), req)

	checkResponseCode(t, http.StatusOK, response.Code)
}

// Rule #4: schedules endpoint should return 200 with schedules as payload when schedules found
func TestServer_schedules_found(t *testing.T) {
	s, closeFunc := newServer()
	defer closeFunc()

	now := time.Now()
	epoch := now.Add(10 * time.Second)
	err := s.Add(simple.NewSchedule("1", epoch, now))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/schedules", http.NoBody)
	response := executeRequest(s.Router(), req)

	checkResponseCode(t, http.StatusOK, response.Code)

	type schedule struct {
		ID        string `json:"id"`
		Epoch     int64  `json:"epoch"`
		Timestamp int64  `json:"timestamp"`
	}

	list := []schedule{}
	err = json.Unmarshal(response.Body.Bytes(), &list)
	if err != nil {
		t.Fatalf("unable to unmarshall json body: %v - %s", err, response.Body.Bytes())
	}

	t.Logf("schedules: %s", response.Body.String())

	if v := len(list); v != 1 {
		t.Errorf("unexpected result length: %v", v)
	}

	sch := list[0]

	if v := sch.ID; v != "1" {
		t.Errorf("unexpected ID: %v", v)
	}
	if v := sch.Epoch; v != sch.Epoch {
		t.Errorf("unexpected epoch: %v", v)
	}
	if v := sch.Timestamp; v != sch.Timestamp {
		t.Errorf("unexpected timestamp: %v", v)
	}
}
