package mini_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/etf1/kafka-message-scheduler/runner/mini"
)

// Rule #1: mini runner must expose the api server endpoint /info
func TestMiniRunner_info(t *testing.T) {
	runner := mini.NewRunner()

	exitchan := make(chan bool)

	go func() {
		if err := runner.Start(); err != nil {
			log.Printf("failed to create the default kafka runner: %v", err)
		}
		exitchan <- true
	}()

	// wait for the goroutine to be scheduled
	time.Sleep(1 * time.Second)

	resp, err := getInfo(1 * time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: %v", resp.StatusCode)
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Logf("http response body: %s", body)

	res := info{}
	err = json.NewDecoder(bytes.NewReader(body)).Decode(&res)
	if err != nil {
		t.Fatalf("unable to unmarshall json body: %v %+v", err, res)
	}

	if v := res.Host; v == "" {
		t.Fatalf("unexpected host: %v", v)
	}
	if v := len(res.kafka.Topics); v == 0 {
		t.Fatalf("unexpected topics: %v", v)
	}
loop:
	for {
		select {
		case <-time.After(2 * time.Second):
			runner.Close()
		case <-exitchan:
			break loop
		}
	}
}

// Rule #2: mini runner must expose the api server endpoint /schedules with minimalist data
func TestMiniRunner_schedules(t *testing.T) {
	runner := mini.NewRunner()

	exitchan := make(chan bool)

	go func() {
		if err := runner.Start(); err != nil {
			log.Printf("failed to create the default kafka runner: %v", err)
		}
		exitchan <- true
	}()
	defer func() {
		runner.Close()
		<-exitchan
	}()

	// wait for the goroutine to be scheduled
	time.Sleep(1 * time.Second)

	var res []schedule
	var err error

	// retry 5 times, and give up
	for i := 1; i <= 5; i++ {
		res, err = getSchedules(1 * time.Second)
		if err != nil {
			t.Logf("get schedules failed: %v", err)
		}

		if v := len(res); v == 0 {
			t.Logf("schedules not found")
		} else {
			t.Logf("schedules found: %v", v)
			break
		}

		time.Sleep(time.Duration(i) * time.Second)
		t.Logf("retries...")
	}

	if v := len(res); v == 0 {
		t.Errorf("unexpected list length: %v", v)
	}
}
