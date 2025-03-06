package server

import (
	Ser "github.com/cloudwego/kitex/server"
	"net"
	"nrMQ/logger"
	"nrMQ/zookeeper"
	"os"
	"runtime"
)

type PartKey struct {
	Name string `json:"Name"`
}

// 初始化Broker时的消息
type Options struct {
	Me                 int
	Name               string
	Tag                string
	ZKServer_Host_Port string
	Broker_Host_Port   string
	Raft_Host_Port     string
}

// Broker向ZKServer发送自己的新能指标，用于按权值负载均衡
type Property struct {
	Name    string `json:"Name"`
	Power   int64  `json:"Power"`
	CPURate int64  `json:"CPURate"`
	DiskIO  int64  `json:"DiskIO"`
}

// Broker启动时获得的初始信息
type BroNodeInfo struct {
	Topics map[string]TopNodeInfo `json:"Topics"`
}

type TopNodeInfo struct {
	Topic_name string
	Part_nums  int
	Partitions map[string]ParNodeInfo
}

type ParNodeInfo struct {
	Part_name  string
	Block_nums int
	Blocks     map[string]BloNodeInfo
}

type BloNodeInfo struct {
	Start_index int64
	End_index   int64
	Path        string
	File_name   string
}

type BrokerS struct {
	BroBrokers map[string]string `json:"brobrokers"`
	RafBrokers map[string]string `json:"rafbrokers"`
	Me_Brokers map[string]int    `json:"me_brokers"`
}

const (
	ZKBROKER = "zkbroker"
	BROKER   = "broker"
)

func NewBrokerAndStart(zkinfo zookeeper.ZkInfo, opt Options) *RPCServer {
	//start the broker server
	addr_bro, _ := net.ResolveTCPAddr("tcp", opt.Broker_Host_Port)
	addr_raf, _ := net.ResolveTCPAddr("tcp", opt.Raft_Host_Port)
	var opts_bro, opts_raf []Ser.Option
	opts_bro = append(opts_bro, Ser.WithServiceAddr(addr_bro))
	opts_raf = append(opts_raf, Ser.WithServiceAddr(addr_raf))

	rpcServer := NewRPCServer(zkinfo)

	go func() {

	}()
}

func CheckFileOrList(path string) (ret bool) {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func CreateList(path string) error {
	ret := CheckFileOrList(path)

	if !ret {
		err := os.Mkdir(path, 0775)
		if err != nil {
			_, file, line, _ := runtime.Caller(1)
			logger.DEBUG(logger.DError, "%v:%v mkdir %v error %v\n", file, line, path, err.Error())
		}
	}

	return nil
}

func CreateFile(path string) (file *os.File, err error) {
	return os.Create(path)
}

func GetBlockName(fileName string) (ret string) {
	ret = fileName[:len(fileName)-4]
	return ret
}
