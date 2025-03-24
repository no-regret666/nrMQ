## Consumer的功能
- 通过Sub订阅Topic或Partition
- 通过Pull和Pub消费信息

## `NewConsumer(zkBroker string, name string, port string) (*Consumer, error)`
consumer调用NewConsumer创建一个consumer，获得一个句柄，通过这个句柄来调用函数
- zkBroker为zkserver的IP和端口，需要连接到zkserver获取topic所在的Broker位置等；
- name为consumer的唯一标识，需要手动设置，注意：需要唯一；
- port为consumer的端口号，consumer将启动一个rpc server，接收Pub模式的消息；

## `(c *Consumer) Start_server()`
consumer调用Start_server启动RPC server

注意：需要新开启一个协程来调用此函数

## `Sub(topic, partition string, option int8) (err error)`
consumer调用Sub来订阅Topic或Partition，该函数调用Sub RPC到zkserver
- 第一个参数是TopicName
- 第二个参数是PartitionName
- 第三个参数是订阅的类型，一共有两个模式PTP和PSB
  - PTP：订阅Topic的所有分片，需要负载均衡，一个信息只被同种订阅的consumer消费一次
  - PSB：订阅具体的Topic-Partition，不需要负载均衡，每个PSB开一个Part来发送消息，可重复消费

## 消费信息
消费信息有两种方式，Pub和Pull，订阅该Topic或Partition后需要先调用StartGet函数，该函数会发送RPC到中zkserver，zkserver将查询哪些Broker
负责该Topic和Partition，向这些Broker发出通知，做好准备，同时返回需要向集群中请求数据的Leaders，且保证这些Broker处于在线状态；当RPC返回后
consumer将连接到这些Broker上，向这些Broker发出自己的信息，这些Broker将会连接到consumer，得到一个RPC句柄。
通过这个句柄将使用Pub模式消费信息。

## 消费模式
- PTP模式，即consumer订阅了整个Topic的信息，且所有订阅PTP模式的consumer只能有一个消费者消费同一条信息
- PSB模式，即consumer订阅了某个具体的Partition，可以指定消费位置

### Pub模式
Pub模式即Broker将主动向consumer发送信息，通过Broker连接Consumer RPC Server获取的句柄，向consumer发送RPC，将信息发给consumer。
Pub模式需要主动在Pub函数中添加需要的代码来处理信息。

### Pull模式
Pull模式即consumer将主动向Broker拉取消息，通过Consumer连接Broker RPC Server时获取的句柄，consumer发送RPC，获取信息，由consumer主动
控制获取信息的量和时间。
consumer使用Pull拉取信息时需要传入info参数，可以通过GetCli函数获取client句柄。
```
type Info struct {
	Offset int64 //需要消费的位置，PSB需要自己设置，PTP不需要设置
	Topic  string
	Part   string
	Option int8 //消费类型，PTP_PULL、PTP_PUSH、PSB_PULL、PSB_PUSH
	Size   int8 
	Cli    *server_operations.Client //Pull信息时需要此项，需要给出Partition所在的Broker的句柄，由StartGet会返回Broker的信息
	Bufs   map[int64]*api.PubRequest
}
```