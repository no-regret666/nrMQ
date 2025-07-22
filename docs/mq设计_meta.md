## meta
meta为元数据，这里主要是对Broker，Topic，Partition，consumer，consumer group之间的关系

## Broker
一个Broker即一个服务器，多个服务器组成一个集群，一个Broker中需要多个Topic，broker采取map[string]topic的方式来存储各个topic

## Topic
topic表示一个主题的消息队列，按道理这里需要维持一个队列，来存储消息，但是我们为了支持水平扩展集群，将一个topic再次分片成更小的Partition，
这里包含一定的partition，每个partition中再维持一个队列，这样就会导致topic中的消息无法保持顺序性，而每个partition中可以保证顺序性

## Partition
为了支持水平扩展集群，来提高mq的性能，我们将topic再次分片管理，首先这里需要维持一个队列，来存储消息（后续需要增加消息的持久化，这里暂时作为
内存消息队列）（持久化是将消息顺序读写到磁盘（文件）中，这里当消息接受到后就将数据写入磁盘（文件），通过将消息按块读出放入此队列，可以减少磁盘
IO，因为是消费者自己维护一个offset来标记位置，所以可以不担心被读出后，又没有被消费导致消息无法被消费者接受到。这里还需要一个细节就是offset
到这个队列块的转换。以topic+partition的方式来命名文件）。

## Subscription
消费模式支持两种：点对点(point to point)和订阅发布（sub and pub)；每个Topic都有可能会有这两种模式，所以每个Topic将拥有两个Subscription，
我们会将这个范围扩大，让每个Partition拥有两个Partition，分别支持这两种方式。当有消费者要订阅的情况分别如下：

point to point：Subscription中只能有一个消费者组，Topic中的一条消息只能被一个消费者消费。我们会将这个范围扩大，每个topic中的一个partition
只能被一个消费者消费。当有消费者选择这个模式时，将判断是否有一个group，若无则创建一个，若有则加入；

sub and pub：Subscription中可以有多个消费者组，每个消费者组中只有一个消费者。

## Client
当Consumer连接到mq后，consumer会发送一个info，让mq通过RPC连接到该客户端，维护一个client，该Client中保留可以发送sub等请求的consumer，
和该客户端所参见的订阅集合，这里采用map的方式：map[string]*Subscription；和一个表示该客户端状态的变量state；

## 消费者组
- consumer group下可以有一个或多个consumer instance，consumer instance可以是一个进程，也可以是一个线程
- group.id是一个字符串，唯一标识一个consumer group
- consumer group下订阅的topic下的每个分区只能分配给某个group下的一个consumer（当然该分区还可以被分配给其他group）       