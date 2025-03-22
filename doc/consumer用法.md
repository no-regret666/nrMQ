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