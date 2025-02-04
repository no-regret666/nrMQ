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

service Server_Operations {
    PushResponse push(1: PushRequest req)
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

service ZkServer_Operation {
    CreateTopicResponse CreateTopic(1: CreateTopicRequest req)
    CreatePartResponse CreatePart(1: CreatePartRequest req)
    ProGetBrokResponse ProGetBroker(1: ProGetBrokRequest req)
    SetPartitionStateResponse SetPartitionState(1: SetPartitionStateRequest req)
}