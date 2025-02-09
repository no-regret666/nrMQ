package server

// 初始化Broker时的消息
type Options struct {
	Me                 int
	Name               string
	Tag                string
	ZKServer_Host_Port string
	Broker_Host_Port   string
	Raft_Host_Port     string
}
