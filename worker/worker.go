package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	topic := "comments"
	worker, err := connectConsumer([]string{"localhost:29092"})
	if err != nil {
		log.Fatal("Error connecting consumer:", err)
	}
	defer worker.Close()

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatal("Error creating partition consumer:", err)
	}
	defer consumer.Close()

	fmt.Println("consumer started")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0
	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)

			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf("Received message Count: %d | Topic(&s) | Message(%s)\n", msgCount, string(msg.Topic), string(msg.Value))
			case <-sigchan:
				fmt.Printf("Interruption detected")
				doneCh <- struct{}{}
				return
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil

}
