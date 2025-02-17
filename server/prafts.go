package server

import (
	"github.com/cloudwego/kitex/server"
	"nrMQ/raft"
	"sync"
)

const (
	TIMEOUT        = 1000 * 1000
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrWrongNum    = "ErrWrongNum"
)

type COMD struct {
	index   int
	The_num int
}

type SnapShot struct {
	Tpart       string
	Csm         map[string]int64
	Cdm         map[string]int64
	Apliedindex int
}

type parts_raft struct {
	mu         sync.RWMutex
	srv_raft   server.Server
	Partitions map[string]*raft.Raft
	Leaders    map[string]bool

	me      int
	append  chan info
	applyCh chan raft.ApplyMsg

	maxraftstate int   //snapshot if log grows this big
	dead         int32 //set by kill()

	Add chan COMD

	CSM map[string]map[string]int64
	CDM map[string]map[string]int64

	ChanComd map[int]COMD //管道getkvs的消息队列

	//多raft,则需要多applyindex
	applyindexs map[string]int

	Now_Num int
}

func NewParts_Raft() *parts_raft {
	return &parts_raft{
		mu: sync.RWMutex{},
	}
}

func (p *parts_raft) Make(name string, opts []server.Option, appench chan info, me int) {
	p.applyCh = appench
	p.me = me

}
