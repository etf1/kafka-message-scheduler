package mini_test

import (
	"context"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/etf1/kafka-message-scheduler/config"
)

func getInfo(timeout time.Duration) (resp *http.Response, err error) {
	return get("/info", timeout)
}

func getSchedules(timeout time.Duration) (resp *http.Response, err error) {
	return get("/schedules", timeout)
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
