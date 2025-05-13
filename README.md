## nrMQ
nrMQ是一款运行于Linux操作系统，由Golang语言开发的分布式的面向消息的中间件，能够实时存储和分发数据。

- 支持多种订阅模式，用户可根据不同场合选择消息的消费模式
- 多种副本同步方式，通过ack机制根据需求选择合适的同步方式
- 支持分布式，可以提高可用性和容错性
- 使用zookeeper存储元数据，保证数据一致性和高可用
- 采用SSTable标记索引位置，加速消息索引查询
- 顺序读写磁盘，提高数据读写效率

## 使用说明
### 安装
1. 拉取nrMQ
```
git clone https://github.com/no-regret666/nrMQ.git
```

2. 使用kitex生成RPC代码
```
cd nrMQ
kitex -module nrMQ operations.thrift
kitex -module nrMQ raftoperations.thrift
```
接下来你就可以在你的代码中使用nrMQ。

### server
将代码中引入nrMQ后，可选择以下操作来运行Server
1. 方式1
    ```
    sh build.sh
    ```
    执行上述命令后应该有一个output目录，其中包括编译产品
    ```
    sh output/bootstrap.sh
    ```
    执行上述命令后Server开始运行

2. 方式2
    ```
    go run main.go
    ```
    详细使用方法请参考[ZKServer使用文档](https://github.com/no-regret666/nrMQ/blob/98942f13bad2a996fd56f79f7616bb8bb3da6ddd/doc/zkserver%E7%94%A8%E6%B3%95.md)
    和[Broker使用文档](https://github.com/no-regret666/nrMQ/blob/98942f13bad2a996fd56f79f7616bb8bb3da6ddd/doc/broker%E7%94%A8%E6%B3%95.md)

### producer
详细使用方法请参考[producer使用文档](https://github.com/no-regret666/nrMQ/blob/98942f13bad2a996fd56f79f7616bb8bb3da6ddd/doc/producer%E7%94%A8%E6%B3%95.md)

### consumer
详细使用方法请参考[consumer使用文档](https://github.com/no-regret666/nrMQ/blob/98942f13bad2a996fd56f79f7616bb8bb3da6ddd/doc/consumer%E7%94%A8%E6%B3%95.md)