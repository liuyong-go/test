package core

import (
	"context"
	"fmt"
	"reflect"

	"github.com/Shopify/sarama"
)

type GroupConsumer struct {
	GroupId  string
	Topics   []string
	CallFunc func(string) error
}

func CreateGroupConsumer(consumer *GroupConsumer) {
	config := sarama.NewConfig()
	client, err := sarama.NewClient(client, config)
	if err != nil {
		panic(fmt.Sprintf("connect error %s", err))
	}
	defer client.Close()
	group, err := sarama.NewConsumerGroupFromClient(consumer.GroupId, client)
	if err != nil {
		panic(fmt.Sprintf("group client error %s", err))
	}
	defer group.Close()
	ctx := context.Background()
	handler := &consumeGroupHandler{name: consumer.GroupId, callfunc: consumer.CallFunc}
	for {
		err := group.Consume(ctx, consumer.Topics, handler)
		if err != nil {
			panic(fmt.Sprintf("group Consume error %s", err))
		}
	}
}

type consumeGroupHandler struct {
	name     string
	callfunc func(string) error
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
		fv := reflect.ValueOf(h.callfunc)
		in := make([]reflect.Value, 1)
		in[0] = reflect.ValueOf(string(msg.Value))
		err := fv.Call(in)[0].Interface()
		if err != nil {
			fmt.Println("BAKC ERR", err)
		}
		// 手动确认消息
		sess.MarkMessage(msg, "mark")
	}
	return nil
}
