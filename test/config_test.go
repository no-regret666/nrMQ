package main

import (
	"fmt"
	"nrMQ/kitex_gen/api/client_operations"
	"nrMQ/server"
	"os"
	"testing"
)

func TestConfigPTPConsistent(t *testing.T) {
	fmt.Println("Test: PTP_Config consumer add and sub")

	topic := "phone_number"
	partitions := []string{"xian", "shanghai", "beijing"}
	consumers := []string{"consumer1", "consumer2", "consumer3", "consumer4", "consumer5"}

	str, _ := os.Getwd()
	var cli *client_operations.Client
	Parts := make(map[string]*server.Partition)
	Files := make(map[string]*server.File)

	for _, name := range partitions {
		path := str + "/" + "Broker" + "/" + topic + "/" + name + "/" + "NowBlock.txt"

		Parts[name] = server.NewPartition("Broker", topic, name)
		file, _, _, err := server.NewFile(path)
		if err != nil {
			t.Fatal(err.Error())
		}
		Files[name] = file
	}

	PTP_Config := server.NewConfig(topic, len(partitions), Parts, Files)

	fmt.Println("PTP_Config add Consumers")
	for _, con := range consumers {
		PTP_Config.AddCli(con, cli)

		ret, ok := CheckNums(PTP_Config.PartToCon, 1, 2, len(partitions))
		if !ok {
			t.Fatal(ret)
		}
	}

	fmt.Println("... Passed")
}
