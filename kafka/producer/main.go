package main

import (
	"encoding/json"
	"test/kafka/core"

	"github.com/Shopify/sarama"
)

type testData struct {
	TestID int    `json:"test_id"`
	Data   string `json:"data"`
}

func (t testData) Tostring() sarama.StringEncoder {
	messageString, _ := json.Marshal(t)
	return sarama.StringEncoder(messageString)
}

func main() {
	client := core.InitProducer()
	defer client.Close()
	core.SendMessage("test", testData{
		TestID: 1,
		Data:   "test",
	})
}
