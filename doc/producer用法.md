## producer用法
### `CreateTopic(topicName string)`
创建Topic，传入topicname

### `CreatePart(partName string)`
创建Part，传入partname

### `Push(msg Message,ack int)`
生产信息，传入msg和ack机制

#### ack机制
ack = 0：表示生产者在成功写入消息之前不会等待任何来自服务器的响应

ack = 1：表示只要集群的leader分区副本接受到了信息，就会向生产者发送一个成功响应的ack，此时生产者接收到ack之后就可以认为该消息是写入成功的。

ack = -1：表示采用raft机制同步主副本间的信息，当大部分副本存入信息后返回Push的RPC