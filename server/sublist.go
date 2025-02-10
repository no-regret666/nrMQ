package server

import (
	"os"
	"sync"
)

type SubScription struct {
	rmu        sync.RWMutex
	name       string
	topic_name string

	option int8 //PTP / PSB

}

type Topic struct {
	mu      sync.RWMutex
	Broker  string
	Name    string
	Files   map[string]*File
	Parts   map[string]*Partition
	subList map[string]*SubScription
}

type Partition struct {
	mu     sync.RWMutex
	Broker string
	key    string
	state  string

	file_name   string
	file        *File
	fd          *os.File
	index       int64
	start_index int64
	queue       []Message
}
