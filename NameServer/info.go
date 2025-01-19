package NameServer

type QueueData struct {
	Name string
}

type BrokerData struct {
	Address string
}

type BrokerLiveInfo struct {
	Status string
}

type RouteInfo struct {
	topicQueueTable   map[string][]QueueData         //topic消息队列的路由信息
	brokerQueueTable  map[string]BrokerData          //Broker基础信息
	clusterAddrTable  map[string]map[string]struct{} //Broker集群信息
	brokerLiveTable   map[string]BrokerLiveInfo      //Broker状态信息
	filterServerTable map[string][]string            //
}
