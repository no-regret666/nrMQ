package server

import "sync"

type File struct {
	mu        sync.RWMutex
	filename  string
	node_size int
}
