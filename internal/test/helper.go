package test

// This package contains reusable code for tests.
// This is why the name of the package is test
import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"testing"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/etf1/kafka-message-scheduler/internal/helper"
	"github.com/etf1/kafka-message-scheduler/schedule/kafka"
	kafka_store "github.com/etf1/kafka-message-scheduler/store/kafka"
)

const (
	adminTimeout   = 120 * time.Second
	timeout        = 10 * time.Second
	randomN        = 1000000
	pollTimeoutMs  = 100
	flushTimeoutMs = 10000
)

func NewKafkaStore(t *testing.T, nbTopic int, nbPartitions []int) (store *kafka_store.Store, topics []string) {
	topics = CreateTopics(t, nbTopic, nbPartitions, "scheduler")
	return NewKafkaStoreFromTopics(t, topics), topics
}

func NewKafkaStoreFromTopics(t *testing.T, topics []string) *kafka_store.Store {
	sessionTimeout := 6000
	store, err := kafka_store.NewStore(nil, helper.GetDefaultBootstrapServers(), topics, "scheduler-cg", sessionTimeout)
	if err != nil {
		t.Fatalf("failed to create kafka store: %v\n", err)
	}

	return store
}

// Creates topics based on the array of nbPartitions param
func CreateTopics(t *testing.T, nbTopic int, nbPartitions []int, prefix string) []string {
	topics := make([]string, nbTopic)

	adm, err := confluent.NewAdminClient(&confluent.ConfigMap{
		"bootstrap.servers": helper.GetDefaultBootstrapServers(),
	})
	if err != nil {
		t.Fatalf("failed to create admin client: %v\n", err)
	}
	defer adm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replicationFactor := 1
	specs := make([]confluent.TopicSpecification, nbTopic)
	for i := 0; i < len(specs); i++ {
		topics[i] = RandomTopicName(prefix)
		specs[i] = confluent.TopicSpecification{
			Topic:             topics[i],
			NumPartitions:     nbPartitions[i],
			ReplicationFactor: replicationFactor}
	}

	results, err := adm.CreateTopics(ctx, specs, confluent.SetAdminOperationTimeout(adminTimeout))
	if err != nil {
		t.Fatalf("failed to create topics %v: %v", topics, err)
	}

	for _, result := range results {
		t.Logf("%s\n", result)
	}

	return topics
}

func genRandNum() int64 {
	bg := big.NewInt(randomN)

	n, err := rand.Int(rand.Reader, bg)
	if err != nil {
		panic(err)
	}

	return n.Int64()
}

// creates a random topic name, each test got a different topic name
func RandomTopicName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, genRandNum())
}

func GetConsumerConfig(prefix string) *confluent.ConfigMap {
	return &confluent.ConfigMap{
		"bootstrap.servers":     helper.GetDefaultBootstrapServers(),
		"broker.address.family": "v4",
		"group.id":              fmt.Sprintf("%s-%d", prefix, genRandNum()),
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest",
	}
}

func GetProducerConfig() *confluent.ConfigMap {
	return &confluent.ConfigMap{
		"bootstrap.servers": helper.GetDefaultBootstrapServers(),
	}
}

// Converts int, string to slice of byte
func toBytes(value interface{}) []byte {
	if value == nil {
		return nil
	}

	k := "unknow type"
	switch kt := value.(type) {
	case int:
		k = strconv.Itoa(kt)
	case string:
		k = kt
	case []byte:
		return kt
	}

	return []byte(k)
}

// Creates a kafka.Message
func Message(topic string, key, value interface{}, epoch int64) *confluent.Message {
	headers := []confluent.Header{
		{
			Key:   kafka.Epoch,
			Value: []byte(strconv.FormatInt(epoch, 10)),
		}}

	return &confluent.Message{
		TopicPartition: confluent.TopicPartition{Topic: &topic, Partition: confluent.PartitionAny},
		Headers:        headers,
		Value:          toBytes(value),
		Key:            toBytes(key),
	}
}

// Creates multiple kafka.Messages
func Messages(topic string, epochs []int64) []*confluent.Message {
	messages := make([]*confluent.Message, 0)

	for i := 0; i < len(epochs); i++ {
		messages = append(messages, Message(topic, i, "some value", epochs[i]))
	}

	return messages
}

// FullMessage creates a kafka message with more details with scheduler headers
func FullMessage(topic string, key, value interface{}, epoch int64, targetTopic string, targetKey interface{}) *confluent.Message {
	headers := []confluent.Header{
		{
			Key:   kafka.Epoch,
			Value: []byte(strconv.FormatInt(epoch, 10)),
		},
		{
			Key:   kafka.TargetTopic,
			Value: []byte(targetTopic),
		},
		{
			Key:   kafka.TargetKey,
			Value: toBytes(targetKey),
		}}

	return &confluent.Message{
		TopicPartition: confluent.TopicPartition{Topic: &topic, Partition: confluent.PartitionAny},
		Headers:        headers,
		Value:          toBytes(value),
		Key:            toBytes(key),
		Timestamp:      time.Now(),
	}
}

func ProduceMessages(t *testing.T, messages []*confluent.Message) {
	p, err := confluent.NewProducer(GetProducerConfig())
	if err != nil {
		t.Fatalf("failed to create producer: %s\n", err)
	}
	defer p.Close()

	deliveryChan := make(chan confluent.Event)
	defer close(deliveryChan)

	for _, message := range messages {
		err := p.Produce(message, deliveryChan)
		if err != nil {
			t.Fatalf("produce failed: %v\n", err)
		}
		e := <-deliveryChan
		m := e.(*confluent.Message)

		if m.TopicPartition.Error != nil {
			t.Fatalf("delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			t.Logf("delivered message to topic %s [%d] at offset %v\n",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}
	}

	p.Flush(flushTimeoutMs)
}

// Compares 2 slices of kafka messages
func AssertEquals(t *testing.T, a, b []*confluent.Message) {
	if len(a) != len(b) {
		t.Fatalf("messages length not equals, %v != %v", len(a), len(b))
	}

	for i, msg := range a {
		AssertMessageEquals(t, msg, b[i])
	}
}

// Compares 2 kafka messages
func AssertMessageEquals(t *testing.T, m1, m2 *confluent.Message) {
	if !reflect.DeepEqual(m1.Headers, m2.Headers) {
		t.Fatalf("headers not equals, %v != %v", m1.Headers, m2.Headers)
	}
	if !bytes.Equal(m1.Key, m2.Key) {
		t.Fatalf("keys not equals, %v != %v", m1.Key, m2.Key)
	}
	if !bytes.Equal(m1.Value, m2.Value) {
		t.Fatalf("values not equals, %v != %v", string(m1.Value), string(m2.Value))
	}
}

// Verifies if specified messages are in the topic
func AssertMessagesinTopic(t *testing.T, topic string, msgs []*confluent.Message) {
	config := GetConsumerConfig("cg-test")
	t.Logf("consumer config: topic=%v %+v", topic, config)

	c, err := confluent.NewConsumer(config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer c.Close()

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	foundCount := 0
	for _, m := range msgs {
		msg, err := c.ReadMessage(timeout)
		if err != nil {
			if err.(confluent.Error).Code() == confluent.ErrTimedOut {
				break
			}
			t.Fatalf("unexpected error: %v", err)
		}
		AssertMessageEquals(t, m, msg)
		foundCount++
	}
	if foundCount != len(msgs) {
		t.Fatalf("unexpected found count %v, expected %v", foundCount, len(msgs))
	}
}

func PrintMessages(t *testing.T, prefix string, msgs []*confluent.Message) {
	for i, msg := range msgs {
		if msg == nil {
			t.Logf("%v: %v: message is nil\n", prefix, i)
		} else {
			t.Logf("%v: %v: value:%v\n", prefix, i, string(msg.Value))
		}
	}
}

// Get messages in the topic (it will return after 10s)
func ConsumeMessages(t *testing.T, topic string) []*confluent.Message {
	c, err := confluent.NewConsumer(GetConsumerConfig("cg-test"))
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer c.Close()

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := make([]*confluent.Message, 0)
	timeout := time.After(timeout)

	for {
		select {
		case <-timeout:
			return result
		default:
			ev := c.Poll(pollTimeoutMs)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *confluent.Message:
				result = append(result, e)
			case confluent.Error:
				t.Fatalf("unexpected error: %v", e)
			default:
				t.Logf("ignoring %v\n", e)
			}
		}
	}
}
