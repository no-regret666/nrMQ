struct CreateTopicRequest {
    1: string topic_name
}

struct CreateTopicResponse {
    1: bool ok
    2: string err
}

struct CreatePartRequest {
    1: string topic_name
    2: string part_name
}

struct CreatePartResponse {
    1: bool ok
    2: string err
}

service ZkServer_Operation {
    CreateTopicResponse CreateTopic(1: CreateTopicRequest req)
    CreatePartResponse CreatePart(1: CreatePartRequest req)
}