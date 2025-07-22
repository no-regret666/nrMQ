## producer功能
- 创建Topic
- 创建Partition
- 向Topic-Partition生产信息
- 设置Partition的状态，选择高可用的集中模式

## `NewProducer(zkBroker string, name string) (*Producer, error)`
在程序中导入nrMQ/client/clients包，使用NewProducer创建一个生产者
- zkbroker是zkserver的IP和端口，producer需要连接到zkserver
- name是producer的唯一ID，需要手动设置，注意：name是唯一标识

## `CreateTopic(topicName string) error`
调用CreateTopic函数，该函数将调用RPC到zkserver，通过zkserver在zookeeper上创建topic节点
- 传入topicname

## `CreatePart(partName string) error`
调用CreatePart函数，该函数将调用RPC到zkserver，通过zkserver在zookeeper上创建partition节点
- 传入partname

## `SetPartitionState(topicName, partName string, option, repNum int8) error`
producer需要设置Partition的状态，也就是ACK机制的其中一种
- -1 采用raft机制同步主副本间的信息，当大部分副本存入信息后返回Push的RPC
- 1 采用fetch机制同步主副本间的信息，当Leader存入信息后就返回Push的RPC
- 0 采用fetch机制同步主副本间的信息，当Broker收到信息后就返回Push的RPC

通过调用SetPartitionState函数，此函数调用RPC到zkserver，zkserver将修改此Partition的状态，若state为1和0之间的转换，则不需要重新设置，
当Push请求发送时会处理。当从raft到fetch转换时，需要停止raft集群，并创建fetch机制，不需要更换存信息的文件。当从fetch到raft转换时，需要停止fetch机制，
将原本的主副本所在的Broker组成raft集群，需要创建新的文件存信息。当从未设置状态到其他状态时需要使用负载均衡，分配3个Broker组成主副本集群来同步信息
（副本均在不同的Broker中）。

- 传入的第一个参数是TopicName
- 传入的第二个参数是PartitionName
- 传入的第三个参数是ACK即state
- 传入的第四个参数是集群中Broker的个数，目前只支持3，后续可增加

## `Push(msg Message,ack int) error`
producer通过Push函数生产信息，使用前需要设置Partition的状态，即上一步。
- 将信息封装成一个结构体，作为第一个参数
```
type Message struct {
	Topic     string //主题
	Partition string //分区
	Msg       []byte //信息
}
```
- ACK机制的选项，需要和上一步设置的状态一致