namespace go api

struct PushRequest {
    1: string producer
    2: string topic
    3: string key
    4: binary message
    5: i64 StartIndex
    6: i64 EndIndex
    7: i8 Size
    8: i8 Ack
    9: i64 CmdIndex
}

struct PushResponse {
    1: bool ret
    2: string err
}

struct InfoRequest {
    1: string ip_port
}

struct InfoResponse {
    1: bool ret
}

//consumer准备开始接收信息
struct InfoGetRequest {
    1: string cli_name
    2: string topic_name
    3: string part_name
    4: i64 offset
    5: i8 option
}

struct InfoGetResponse {
    1: bool ret
}

struct PullRequest {
    1: string consumer
    2: string topic
    3: string key
    4: i64 offset
    5: i8 size
    6: i8 option
}

struct PullResponse {
    1: binary Msgs
    2: bool Ret
    3: i64 Start_index
    4: i64 End_index
    5: i8 Size
    6: string Err
}

//设置某个Partition接收信息的文件和队列
struct StartGetMessageRequest {
    1: string topic_name
    2: string part_name
    3: string file_name
}

struct StartGetMessageResponse {
    1: bool ret
}

//关闭某个Partition，停止接收信息
struct CloseGetMessageRequest {
    1: string topic_name
    2: string part_name
    3: string file_name
    4: string new_name
}

struct CloseGetMessageResponse {
    1: bool ret
}

//zkserver
struct PrepareAcceptRequest {
    1: string topic_name
    2: string part_name
    3: string file_name
}

struct PrepareAcceptResponse {
    1: bool ret
    2: string err
}

service Server_Operations {
    //producer used
    PushResponse push(1: PushRequest req)

    //consumer used
    InfoResponse ConInfo(1: InfoRequest req)
    InfoGetResponse StartToGet(1: InfoGetRequest req)
    PullResponse Pull(1: PullRequest req)

    //zkserver used this rpc to request broker server
    PrepareAcceptResponse PrepareAccept(1: PrepareAcceptRequest req)
}

struct CreateTopicRequest {
    1: string topic_name
}

struct CreateTopicResponse {
    1: bool ret
    2: string err
}

struct CreatePartRequest {
    1: string topic_name
    2: string part_name
}

struct CreatePartResponse {
    1: bool ret
    2: string err
}

struct ProGetBrokRequest {
    1: string topic_name
    2: string part_name
}

struct ProGetBrokResponse {
    1: bool ret
    2: string broker_host_port
    3: string err
}

struct SetPartitionStateRequest {
    1: string topic
    2: string partition
    3: i8 option
    4: i8 dupnum
}

struct SetPartitionStateResponse {
    1: bool ret
    2: string err
}

struct SubRequest {
    1: string consumer
    2: string topic
    3: string key
    4: i8 option
}

struct SubResponse {
    1: bool ret
}

struct ConStartGetBrokRequest {
    1: string cli_name
    2: string topic_name
    3: string part_name
    4: i8 option
    5: i64 index
}

struct ConStartGetBrokResponse {
    1: bool ret
    2: i64 size
    3: binary parts
}

service ZkServer_Operation {
    CreateTopicResponse CreateTopic(1: CreateTopicRequest req)
    CreatePartResponse CreatePart(1: CreatePartRequest req)
    ProGetBrokResponse ProGetBroker(1: ProGetBrokRequest req)
    SetPartitionStateResponse SetPartitionState(1: SetPartitionStateRequest req)
    SubResponse Sub(1: SubRequest req)
    ConStartGetBrokResponse ConStartGetBroker(1: ConStartGetBrokRequest req)
}

struct PubRequest {
    1: string topic_name
    2: string part_name
    3: i64 start_index
    4: i64 end_index
    5: binary msg
}

struct PubResponse {
    1: bool ret
}

struct PingPongRequest {
    1: bool ping
}

struct PingPongResponse {
    1: bool pong
}

service Client_Operations {
    PubResponse pub(1: PubRequest req)
    PingPongResponse pingpong(1: PingPongRequest req)
}