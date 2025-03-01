package raft

import (
	"reflect"
	"sync"
)

var mu sync.Mutex
var errorCount int
var checked map[reflect.Type]bool
