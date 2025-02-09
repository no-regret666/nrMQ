package server

import "sync"

type SubScription struct {
	rmu        sync.RWMutex
	name       string
	topic_name string

	option int8 //PTP / PSB

}
