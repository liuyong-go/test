package core

import (
	"fmt"

	"github.com/Shopify/sarama"
)

var producer sarama.SyncProducer
var client = []string{"localhost:9092"}

type TopicContent interface {
	Tostring() sarama.StringEncoder
}

func SendMessage(topicName string, message TopicContent) error {
	value := message.Tostring()
	mesage := &sarama.ProducerMessage{Topic: topicName, Value: value}
	pati, offset, err := producer.SendMessage(mesage)
	fmt.Println("send message", pati, offset, err)
	return err
}

func InitProducer() sarama.Client {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	client, err := sarama.NewClient(client, config)
	if err != nil {
		panic(err)
	}
	// defer client.Close()
	producer, err = sarama.NewSyncProducerFromClient(client)
	if err != nil {
		panic(err)
	}
	return client
}
