package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

var client = []string{"0.0.0.0:9092"}

const topicTest = "test"

func main() {
	config := sarama.NewConfig()
	client, err := sarama.NewClient(client, config)
	if err != nil {
		panic(fmt.Sprintf("connect error %s", err))
	}
	defer client.Close()
	consumer, err := sarama.NewConsumerFromClient(client)

	if err != nil {
		panic(fmt.Sprintf("new consumer error %s", err))
	}
	defer consumer.Close()
	// get partitionId list
	partitions, err := consumer.Partitions(topicTest)
	if err != nil {
		panic(fmt.Sprintf("get partitions error %s", err))
	}
	for _, partitionId := range partitions {
		// create partitionConsumer for every partitionId
		partitionConsumer, err := consumer.ConsumePartition(topicTest, partitionId, sarama.OffsetNewest)
		if err != nil {
			panic(fmt.Sprintf("consume partitions error %s", err))
		}

		go func(pc *sarama.PartitionConsumer) {
			defer (*pc).Close()
			// block
			for message := range (*pc).Messages() {
				value := string(message.Value)
				log.Printf("Partitionid: %d; offset:%d, value: %s\n", message.Partition, message.Offset, value)
			}

		}(&partitionConsumer)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
	fmt.Println("consumer shut down")
}
