package mini_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/etf1/kafka-message-scheduler/config"
)

type schedule struct {
	ID          string `json:"id"`
	Epoch       int64  `json:"epoch"`
	Timestamp   int64  `json:"timestamp"`
	TargetTopic string `json:"target-topic"`
	TargetKey   string `json:"target-key"`
	Topic       string `json:"topic"`
	Value       []byte `json:"value"`
}

func getInfo(timeout time.Duration) (*http.Response, error) {
	return get("/info", timeout)
}

func getSchedules(timeout time.Duration) ([]schedule, error) {
	resp, err := get("/schedules", timeout)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed with status code: %v", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("readall failed: %v", err)
	}

	res := []schedule{}
	err = json.NewDecoder(bytes.NewReader(body)).Decode(&res)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshall json body: %v %+v", err, res)
	}

	return res, nil
}

func get(path string, timeout time.Duration) (*http.Response, error) {
	addr := os.Getenv("SERVER_ADDR")
	if addr == "" {
		addr = config.ServerAddr()
	}

	if strings.HasPrefix(addr, ":") {
		addr = "localhost" + addr
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()

	url := "http://" + addr + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Timeout: timeout,
	}

	return client.Do(req)
}
