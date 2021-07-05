package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	tc "github.com/testcontainers/testcontainers-go"
	"log"

	"context"
	"strings"
	"time"
)

var messageNumber int = 0

func main() {
	context.Background()

	composePath := []string{"configs/kafka-docker-compose.yml"}

	fmt.Println("Welcome to my Kafka test...")

	kafka := tc.NewLocalDockerCompose(
		composePath,
		strings.ToLower(uuid.New().String()),
	)
	execError := kafka.WithCommand([]string{"up", "-d"}).Invoke()

	err := execError.Error
	if err != nil {
		log.Fatalln("Docker died...")
	}
	defer destroyKafka(kafka)

	// Find sexier way of figuring out if t he container is started or not.
	sleepSome(13)

	// Start producing messages here.
	//   Find sexier way of figuring out the host and ports for the container.
	producer, pErr := createKafkaProducer([]string{"localhost:9092"})
	if pErr != nil {
		fmt.Printf("Error creating producer: %v\n", pErr)
	}
	defer producer.AsyncClose()

	consumer, cErr := sarama.NewConsumer([]string{"localhost:9092"}, sarama.NewConfig())
	if cErr != nil {
		fmt.Printf("Error creating consumer: %v\n", cErr)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition("test-topic", 0, sarama.OffsetNewest)

	// Produce a message
	fmt.Println("Producing messages...")
	for i := 0; i < 10; i++ {
		producer.Input() <- generateMessage("test-topic")
	}

	sleepSome(5)
	fmt.Println("Start consuming messages...")
	// Consume messages
	for i := 0; i < 10; i++ {
		msg := <-partitionConsumer.Messages()

		fmt.Printf("Message: %s\n  Topic: %s\n  Partition: %d\n  Offset: %d\n", string(msg.Value), msg.Topic, msg.Partition, msg.Offset)
	}

}

func destroyKafka(compose *tc.LocalDockerCompose) {

	fmt.Println("Pulling down docker, sleeping for a second...")
	compose.Down()
	time.Sleep(1 * time.Second)
}

func sleepSome(sleep int) {

	fmt.Printf("Sleeping for %d seconds", sleep)
	for i := 0; i < sleep; i++ {
		fmt.Print(".")
		time.Sleep(time.Second)
	}

	fmt.Println("")
}

func createKafkaProducer(brokerList []string) (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Flush.Frequency = 500 * time.Millisecond

	producer, err := sarama.NewAsyncProducer(brokerList, config)

	if err != nil {
		return nil, err
	}

	return producer, nil
}

func generateMessage(topic string) (message *sarama.ProducerMessage) {
	message = &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(fmt.Sprintf("Test message %d", messageNumber))}
	messageNumber++

	return
}
