package helper

import (
	"fmt"
	"os"
)

// tells if the tests is running in docker
func IsRunningInDocker() bool {
	if _, err := os.Stat("/.dockerenv"); os.IsNotExist(err) {
		return false
	}

	return true
}

// Get the bootstrap servers because in or out the docker the kafka server is different
func GetDefaultBootstrapServers() string {
	if IsRunningInDocker() {
		fmt.Println("kafka bootstrap servers=kafka:29092")
		return "kafka:29092"
	}
	fmt.Println("kafka bootstrap servers=localhost:9092")
	return "localhost:9092"
}
