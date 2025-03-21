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