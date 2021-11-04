package config

import (
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

func getString(name, defaultValue string) string {
	value, set := os.LookupEnv(name)
	if set {
		return value
	}
	return defaultValue
}

func getInt(name string, defaultValue int) int {
	value, set := os.LookupEnv(name)
	if !set {
		return defaultValue
	}
	i, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return i
}

func LogLevel() log.Level {
	lvl, err := log.ParseLevel(getString("LOG_LEVEL", "info"))
	if err != nil {
		return log.InfoLevel
	}
	return lvl
}

func GraylogServer() string {
	return getString("GRAYLOG_SERVER", "")
}

func MetricsAddr() string {
	return getString("METRICS_ADDR", ":8001")
}

func ServerAddr() string {
	return getString("SERVER_ADDR", ":8000")
}

func BootstrapServers() string {
	return getString("BOOTSTRAP_SERVERS", "localhost:9092")
}

func GroupID() string {
	return getString("GROUP_ID", "scheduler-cg")
}

func SessionTimeout() int {
	return getInt("SESSION_TIMEOUT", 6000)
}

func SinceDelta() int {
	return getInt("SINCE_DELTA", 0)
}

func SchedulesTopics() []string {
	return strings.Split(getString("SCHEDULES_TOPICS", "schedules"), ",")
}

func HistoryTopic() string {
	return getString("HISTORY_TOPIC", "history")
}

func OpenTelemetryCollectorEndpoint() string {
	return getString("OTEL_COLLECTOR_ENDPOINT", "")
}
