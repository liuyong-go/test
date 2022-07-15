package main

import (
	"fmt"
	"test/kafka/core"
)

func main() {
	consumer := &core.GroupConsumer{
		GroupId:  "group1",
		Topics:   []string{"test"},
		CallFunc: getMessage,
	}
	core.CreateGroupConsumer(consumer)
}
func getMessage(msg string) error {
	fmt.Println("callback message", msg)
	return fmt.Errorf("test error data")
	//return nil
}
