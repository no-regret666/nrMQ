package main

import (
	"fmt"
	"nrMQ/client/clients"
	"testing"
	"time"
)

func TestProducerCreate(t *testing.T) {
	fmt.Println("Test: producer Create Topic and Partition")

	zkServer := StartZKServer(t)
	time.Sleep(1 * time.Second)

	brokers := StartBrokers(t, 3)
	time.Sleep(1 * time.Second)

	producer := NewProducerAndStart(t, ":7878", "producer1")
	time.Sleep(1 * time.Second)

	fmt.Println("Producer Create a Topic")
	err := producer.CreateTopic("phone_number")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println("Producer Create a Topic/Partition")
	err = producer.CreatePart("phone_number", "wang")
	if err != nil {
		t.Fatal(err.Error())
	}

	ShutDownBrokers(brokers)
	ShutDownZKServer(zkServer)
	fmt.Println(" ...Passed")
}

func TestProducerPush(t *testing.T) {
	fmt.Println("Test:producer push message to partition")

	messages := []string{
		"12345678911", "12345678912", "12345678913",
		"12345678914", "12345678915", "12345678916",
		"12345678917", "12345678918", "12345678919",
	}
	zkServer := StartZKServer(t)
	time.Sleep(1 * time.Second)

	brokers := StartBrokers(t, 3)
	time.Sleep(1 * time.Second)

	producer := NewProducerAndStart(t, ":7878", "producer1")
	time.Sleep(1 * time.Second)

	//创建三个topic
	fmt.Println("Producer Create a Topic")
	err := producer.CreateTopic("phone_number")
	if err != nil {
		t.Fatal(err.Error())
	}

	//每个topic创建三个partition
	fmt.Println("Producer Create a Topic/Partition")
	err = producer.CreatePart("phone_number", "wang")
	if err != nil {
		t.Fatal(err.Error())
	}

	//将partition设置状态
	err = producer.SetPartitionState("phone_number", "wang", -1, 3)
	if err != nil {
		t.Fatal(err.Error())
	}

	//等待领导者选出
	time.Sleep(5 * time.Second)

	for _, message := range messages {
		err = producer.Push(clients.Message{
			Topic:     "phone_number",
			Partition: "wang",
			Msg:       []byte(message),
		}, -1)
	}

	time.Sleep(time.Second * 30)

	ShutDownBrokers(brokers)
	ShutDownZKServer(zkServer)
	fmt.Println("... Passed")
}
