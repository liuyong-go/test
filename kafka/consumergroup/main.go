package main

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
)

var client = []string{"0.0.0.0:9092"}

var topics = []string{"test"}

const groupId = "group1"

type consumeGroupHandler struct {
	name string
}

func (h consumeGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	fmt.Println("Setup up", h.name)
	return nil
}
func (h consumeGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	fmt.Println("clean up", h.name)
	return nil
}
func (h consumeGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("%s Message topic:%q partition:%d offset:%d  value:%s\n", h.name, msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		// 手动确认消息
		sess.MarkMessage(msg, "mark")
	}
	return nil
}

func main() {
	config := sarama.NewConfig()
	client, err := sarama.NewClient(client, config)
	if err != nil {
		panic(fmt.Sprintf("connect error %s", err))
	}
	defer client.Close()
	group, err := sarama.NewConsumerGroupFromClient(groupId, client)
	if err != nil {
		panic(fmt.Sprintf("group client error %s", err))
	}
	defer group.Close()
	ctx := context.Background()
	handler := &consumeGroupHandler{name: groupId}
	for {
		err := group.Consume(ctx, topics, handler)
		if err != nil {
			panic(fmt.Sprintf("group Consume error %s", err))
		}
	}

}
