namespace FsKafka

module Protocol =
  (* For protocol description please refer to https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol *)

  type CoreApi =
    | Metadata
    | Send
    | Fetch
    | Offsets
    | OffsetCommit
    | OffsetFetch

  // not implemented yet
  type GroupManagementApi =
    | GroupCoordinator
    | JoinGroup
    | SyncGroup
    | Heartbeat
    | LeaveGroup

  // not implemented yet
  type AdministrativeApi =
    | DescribeGroups
    | ListGroups
    
  type MessageCodec =
    | None = 0x00
    | GZIP = 0x40
    | Snappy = 0x80
  type Message =
    { Crc:                int32 // crc32 to check the integrity
      MagicByte:          int8
      Attributes:         MessageCodec
      Key:                byte[]
      Value:              byte[] }
  type MessageSetEntry =
    { Offset:             int64
      MessageSize:        int32
      Message:            Message }
  type MessageSet = MessageSetEntry list // not a usual list, as it doesn't have int32 size at the beginning
  (* Compression: batch of Message wrapped into MessageSet => compressed with codec => stored into Message as `value` *)
  (* MessageSet should contain only one compressed message *)

  type MetadataRequest =
    { TopicName:          string list }

  type ProduceTopicPayload =
    { Partition:          int32
      MessageSetSize:     int32
      MessageSet:         MessageSet }
  type ProduceRequestPayload =
    { TopicName:          string
      TopicPayload:       ProduceTopicPayload list }
  type ProduceRequest =
    { RequiredAcks:       int16
      Timeout:            int32
      Payload:            ProduceRequestPayload list }

  type FetchTopicPayload =
    { Partition:          int32
      FetchOffset:        int64
      MaxBytes:           int32 }
  type FetchRequestPayload =
    { TopicName:          string
      TopicPayload:       FetchTopicPayload }
  type FetchRequest =
    { ReplicaId:          int32
      MaxWaitTime:        int32
      MinBytes:           int32
      Payload:            FetchRequestPayload }

  type OffsetRequestTopicPayload =
    { Partition:          int32
      Time:               int64
      MaxNumberOfOffsets: int32 }
  type OffsetRequestTopic =
    { TopicName:          string
      TopicPayload:       OffsetRequestTopicPayload list }
  type OffsetRequest =
    { ReplicaId:          int32
      Topics:             OffsetRequestTopic list }

  type OffsetCommitRequestTopicPayload =
    { Partition:          int32
      Offset:             int64
      Metadata:           string }
  type OffsetCommitRequestPayload =
    { TopicName:          string
      Payload:            OffsetCommitRequestTopicPayload list }
  type OffsetCommitRequest =
    { ConsumerGroup:      string
      Payload:            OffsetCommitRequestPayload list }

  type OffsetFetchRequestTopic =
    { TopicName:          string
      Partitions:         int32 list }
  type OffsetFetchRequest =
    { ConsumerGroup:      string
      Topics:             OffsetFetchRequestTopic list }

  type Broker =
    { NodeId:             int32
      Host:               string
      Port:               int32 }
  type PartitionMetadata =
    { PartitionErrorCode: int16
      PartitionId:        int32
      Leader:             int32
      Replicas:           int32 list
      Isr:                int32 list }
  type TopicMetadata =
    { TopicErrorCode:     int16
      TopicName:          string
      PartitionMetadata:  PartitionMetadata list }
  type MetadataResponse =
    { Broker:             Broker list
      TopicMetadata:      TopicMetadata list }

  type TopicProducedPayload =
    { Partition:          int32
      ErrorCode:          int16
      Offset:             int64 }
  type ProduceResponsePayload =
    { TopicName:          string
      TopicPayload:       TopicProducedPayload list }
  type ProduceResponse = ProduceResponsePayload list

  type FetchResponseTopicPayload =
    { Partition:          int32
      ErrorCode:          int16
      HighwaterMarkOffset:int64
      MessageSetSize:     int32
      MessageSet:         MessageSet }
  type FetchResponsePayload =
    { TopicName:          string
      Payload:            FetchResponseTopicPayload list }
  type FetchResponse = FetchResponsePayload list

  type PartitionOffset =
    { Partition:          int32
      ErrorCode:          int16
      Offsets:            int64 list }
  type OffsetResponseTopic =
    { TopicName:          string
      PartitionOffsets:   PartitionOffset list }
  type OffsetResponse = OffsetResponseTopic list

  type OffsetCommitResponseTopicPayload =
    { Partition:          int32
      ErrorCode:          int16 }
  type OffsetCommitResponseTopic =
    { TopicName:          string
      Payload:            OffsetCommitResponseTopicPayload list }
  type OffsetCommitResponse = OffsetCommitResponseTopic list

  type OffsetFetchResponseTopicPayload = {
    Partition:          int32
    Offset:             int64
    Metadata:           string
    ErrorCode:          int16 }
  type OffsetFetchResponseTopic = {
    TopicName:          string
    Payload:            OffsetFetchResponseTopicPayload list }
  type OffsetFetchResponse = OffsetFetchResponseTopic list

  type RequestType =
    | MetadataRequest of MetadataRequest
    | ProduceRequest of ProduceRequest
    | FetchRequest of FetchRequest
    | OffsetRequest of OffsetRequest
    | OffsetCommitRequest of OffsetCommitRequest
    | OffsetFetchRequest of OffsetFetchRequest
  type ResponseType =
    | MetadataResponse of MetadataResponse
    | ProduceResponse of ProduceResponse
    | FetchResponse of FetchResponse
    | OffsetResponse of OffsetResponse
    | OffsetCommitResponse of OffsetCommitResponse
    | OffsetFetchResponse of OffsetFetchResponse

  type RequestMessage =
    { ApiKey:             int16
      ApiVersion:         int16
      CorrelationId:      int32
      ClientId:           string
      RequestMessage:     RequestType }
  type ResponseMessage =
    { CorrelationId:      int32
      ResponseMessage:    ResponseType }

  type RequestOrResponseType =
    | RequestMessage of RequestMessage
    | ResponseMessage of ResponseMessage
  type RequestOrResponse =
    { Size:               int32
      Message:            RequestOrResponseType }
