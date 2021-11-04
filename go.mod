module github.com/etf1/kafka-message-scheduler

go 1.15

require (
	github.com/confluentinc/confluent-kafka-go v1.5.2
	github.com/gemnasium/logrus-graylog-hook v2.0.7+incompatible
	github.com/gorilla/mux v1.7.3
	github.com/prometheus/client_golang v1.8.0
	github.com/sirupsen/logrus v1.7.0
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/contrib v1.1.0
	go.opentelemetry.io/otel v1.1.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.1.0
	go.opentelemetry.io/otel/sdk v1.1.0
	go.opentelemetry.io/otel/trace v1.1.0
	google.golang.org/grpc v1.42.0
)
