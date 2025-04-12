## zkserver功能
- 为clients和brokers提供meta服务
- 动态扩容brokers时分配broker集群
- 为消息订阅发布等功能准备

## NewRPCServer
在程序中导入nrMQ/server和nrMQ/zookeeper包，使用NewRPCServer创建一个zkserver。
```
broker := server.NewBrokerAndStart(zookeeper.ZkInfo{
			HostPorts: zookeeper_port,
			Timeout:   20,
			Root:      "/nrMQ",
		}, server.Options{
			Me:                 index,
			Name:               "Broker" + strconv.Itoa(index),
			Tag:                server.BROKER,
			Broker_Host_Port:   server_ports[index],
			Raft_Host_Port:     raft_ports[index],
			ZKServer_Host_Port: ":7878",
		})
```
其中ZkInfo是连接zookeeper需要的信息，Options是zkserver的一些信息，如下：
```
type ZkInfo struct {
	HostPorts []string //zookeeper的IP和端口信息
	Timeout   int      //zookeeper连接的超时时间
	Root      string //zookeeper中nrMQ的位置
}

type Options struct {
	Me                 int //启动broker server时需要
	Name               string //zkserver的唯一标识
	Tag                string //该server是zkserver还是broker server的标记
	ZKServer_Host_Port string
	Broker_Host_Port   string
	Raft_Host_Port     string
}
```