package server

import (
	"github.com/cloudwego/kitex/server"
	"nrMQ/kitex_gen/raftoperations/raft_operations"
	"nrMQ/logger"
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
	appench chan info
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
	p.appench = appench
	p.me = me
	p.applyCh = make(chan raft.ApplyMsg)
	p.Add = make(chan COMD)

	p.CDM = make(map[string]map[string]int64)
	p.CSM = make(map[string]map[string]int64)
	p.Partitions = make(map[string]*raft.Raft)
	p.applyindexs = make(map[string]int)
	p.Leaders = make(map[string]bool)

	srv_raft := raft_operations.NewServer(p, opts...)
	p.srv_raft = srv_raft

	err := srv_raft.Run()
	if err != nil {
		logger.DEBUG_RAFT(logger.DError, "the raft run fail %v\n", err.Error())
	}
}
