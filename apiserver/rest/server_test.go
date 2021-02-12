package rest_test

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestServer_isalive(t *testing.T) {
	s, closeFunc := newServer()
	defer closeFunc()

	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/liveness", nil)
	response := executeRequest(s.Router(), req)

	checkResponseCode(t, http.StatusOK, response.Code)
}
