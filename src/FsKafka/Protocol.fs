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

  type ErrorResponseCode =
    | Unknown                             = -1
    | NoError                             = 0
    | OffsetOutOfRange                    = 1
    | InvalidMessage                      = 2
    | UnknownTopicOrPartition             = 3
    | InvalidMessageSize                  = 4
    | LeaderNotAvailable                  = 5
    | NotLeaderForPartition               = 6
    | RequestTimedOut                     = 7
    | BrokerNotAvailable                  = 8
    | ReplicaNotAvailable                 = 9
    | MessageSizeTooLarge                 = 10
    | StaleControllerEpochCode            = 11
    | OffsetMetadataTooLargeCode          = 12
    | OffsetsLoadInProgressCode           = 14
    | ConsumerCoordinatorNotAvailableCode = 15
    | NotCoordinatorForConsumerCode       = 16
    
  type MessageCodec =
    | None   = 0x00
    | GZIP   = 0x01
    | Snappy = 0x02
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
    | MetadataRequest     of MetadataRequest
    | ProduceRequest      of ProduceRequest
    | FetchRequest        of FetchRequest
    | OffsetRequest       of OffsetRequest
    | OffsetCommitRequest of OffsetCommitRequest
    | OffsetFetchRequest  of OffsetFetchRequest
  
  type ResponseType =
    | MetadataResponse     of MetadataResponse
    | ProduceResponse      of ProduceResponse
    | FetchResponse        of FetchResponse
    | OffsetResponse       of OffsetResponse
    | OffsetCommitResponse of OffsetCommitResponse
    | OffsetFetchResponse  of OffsetFetchResponse

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

  module Optics =
    let withCorrelator correlator message =
      match message.Message with
      | RequestMessage  r -> { message with Message = { r with CorrelationId = correlator } |> RequestMessage }
      | ResponseMessage r -> { message with Message = { r with CorrelationId = correlator } |> ResponseMessage }

    let getMetadataResponse requestOrResponse =
      match requestOrResponse.Message with
      | ResponseMessage r ->
          match r.ResponseMessage with
          | MetadataResponse r -> Some r
          | _                  -> None
      | RequestMessage  _ -> None
    
  module Request =

    (* make requests:
        + produce
        - fetch
        - offset
        + metadata
        - offset commit
        - offset fetch
       pickles for requests:
        + produce
        - fetch
        - offset
        + metadata
        - offset commit
        - offset fetch
       compression for produce - done but needs checks. *)

    let private apiVersion = 0s
    let private messageVersion = int8 0
    let private encodeTimeCreation = 0

    let private toApiKey = function
      //| LeaderAndIsr         _ -> 4s
      //| StopReplica          _ -> 5s
      //| ConsumerMetadataRequest _ -> 10s
      | ProduceRequest       _ -> 0s
      | FetchRequest         _ -> 1s
      | OffsetRequest        _ -> 2s
      | MetadataRequest      _ -> 3s
      | OffsetCommitRequest  _ -> 8s
      | OffsetFetchRequest   _ -> 9s

    let private input size message =
      { RequestOrResponse.Size = size
        Message = message }

    let private request clientId correlationId message =
      { ApiKey         = message |> toApiKey
        ApiVersion     = apiVersion
        CorrelationId  = correlationId
        ClientId       = clientId
        RequestMessage = message }
      |> RequestOrResponseType.RequestMessage
      |> input encodeTimeCreation

    let metadata clientId correlationId topics =
      { MetadataRequest.TopicName = topics }
      |> RequestType.MetadataRequest
      |> request clientId correlationId

    // message = (Key * Value) list
    let mkMessageSet codec messages : MessageSet =
      let mapMessage (key, value) =
        { Crc         = encodeTimeCreation
          MagicByte   = messageVersion
          Attributes  = codec
          Key         = key
          Value       = value }
      let wrap message =
        { Offset      = 0L
          MessageSize = encodeTimeCreation
          Message     = message }
      List.map (mapMessage >> wrap) messages

    // payload = (topic * ( (partitionId * MessageSet) list ) ) list
    // messages |> groupBy topic |> each groupBy partition |> each map mkMessageSet
    // to use codecs:
    // grouped messages |> mkMessageSet MessageCodec.None |> encode >> useCodec |> fun x -> [], x |> mkMessageSet codec
    let produce clientId correlationId acks timeout payload =
      let mapPartitions (partitionId, messages:MessageSet) : ProduceTopicPayload =
        { Partition                 = partitionId
          MessageSetSize            = messages.Length
          MessageSet                = messages }
      let mapPayload (topic, topicPayload) : ProduceRequestPayload =
        { TopicName                 = topic
          TopicPayload              = topicPayload |> List.map mapPartitions }

      { ProduceRequest.RequiredAcks = acks
        Timeout                     = timeout
        Payload                     = payload |> List.map mapPayload }
      |> RequestType.ProduceRequest
      |> request clientId correlationId
      
    open Pickle

    let stub = Pickle.pUnit [||]

    let private encodeMetadataRequest (r:MetadataRequest) =
      pList pString r.TopicName

    let private encodeMessage (m:Message) =
      let messageDataPickler (m:Message) = (pInt8 m.MagicByte) >> (pInt8 (m.Attributes |> int8)) >> (pBytes m.Key) >> (pBytes m.Value)
      let messageData = encode messageDataPickler m
      let crc = Crc32.calculate messageData |> int32
      (pInt32 crc) >> (pUnit messageData)

    let private encodeMessageSetEntry (e:MessageSetEntry) =
      (pInt64 e.Offset) >> (pInt32 e.MessageSize) >> (encodeMessage e.Message)

    let private encodeMessageSet v =
      let f v s = List.fold(fun s e -> encodeMessageSetEntry e s) s v
      f v

    let private compress (codec:MessageCodec) (s:MessageSet) =
      let wrap compression =
        let message =
          { Crc         = encodeTimeCreation
            MagicByte   = messageVersion
            Attributes  = codec
            Key         = [||]
            Value       = s |> encode encodeMessageSet |> compression }
        [ { Offset      = 0L
            MessageSize = encodeTimeCreation
            Message     = message } ]
        |> fun s -> (pInt32 1) >> (encodeMessageSet s)
      match codec with
      | MessageCodec.None   -> (pInt32 s.Length) >> (encodeMessageSet s)
      | MessageCodec.GZIP   -> wrap Compression.gzipCompress
      | MessageCodec.Snappy -> wrap Compression.snappyCompress
      | c                   -> sprintf "Unsupported compression %A" c |> failwith

    let private encodeProduceTopicPayload (p:ProduceTopicPayload) =
      let codec = p.MessageSet.[0].Message.Attributes
      (pInt32 p.Partition) >> (compress codec p.MessageSet)

    let private encodeProduceRequestPayload (p:ProduceRequestPayload) =
      (pString p.TopicName) >> (pList encodeProduceTopicPayload p.TopicPayload)

    let private encodeProduceRequest (r:ProduceRequest) =
      (pInt16 r.RequiredAcks) >> (pInt32 r.Timeout) >> (pList encodeProduceRequestPayload r.Payload)

    let private encodeRequestMessage = function
      | MetadataRequest     r -> encodeMetadataRequest r
      | ProduceRequest      r -> encodeProduceRequest r
      | FetchRequest        _ -> stub
      | OffsetRequest       _ -> stub
      | OffsetCommitRequest _ -> stub
      | OffsetFetchRequest  _ -> stub

    let private encodeRequest r =
      (pInt16 r.ApiKey) >> (pInt16 r.ApiVersion) >> (pInt32 r.CorrelationId) >> (pString r.ClientId) >> (encodeRequestMessage r.RequestMessage)

    let encode message =
      match message.Message with
      | RequestMessage r ->
          let data = Pickle.encode encodeRequest r
          let pickler = fun _ -> (pInt32 data.Length) >> (pUnit data)
          Pickle.encode pickler message
      | _                -> failwith "no encode for response needed"
      
  module Response =

    (* make responses:
        + produce
        - fetch
        - offset
        + metadata
        - offset commit
        - offset fetch
       unpicklers for responses:
        + produce
        - fetch
        - offset
        + metadata
        - offset commit
        - offset fetch *)

    open Common
    open Unpickle

    let inline private toResult f = function
      | Failure err       -> sprintf "%A" err |> exn |> Common.Failure
      | Success (data, _) -> f data |> Common.Success
    
    (* Metadata response *)
    let private mkBroker (nodeId, host, port) : Broker =
      { NodeId             = nodeId
        Host               = host
        Port               = port }
    let private mkPartition (errorCode, partitionId, leader, replicas, isr) : PartitionMetadata =
      { PartitionErrorCode = errorCode
        PartitionId        = partitionId
        Leader             = leader
        Replicas           = replicas
        Isr                = isr }
    let private mkTopics (errorCode, name, partitions) : TopicMetadata =
      { TopicErrorCode     = errorCode
        TopicName          = name
        PartitionMetadata  = partitions |> List.map mkPartition }
    let private mkMetadata (brokers, topics) : MetadataResponse =
      { Broker             = brokers    |> List.map mkBroker
        TopicMetadata      = topics     |> List.map mkTopics }

    let private brokerUnpickler            = upTriple upInt32 upString upInt32
    let private partitionMetadataUnpickler = upQuintuple upInt16 upInt32 upInt32 (upList upInt32) (upList upInt32)
    let private topicMetadataUnpickler     = upTriple upInt16 upString (upList partitionMetadataUnpickler)
    let private metadataUnpickler          = upPair (upList brokerUnpickler) (upList topicMetadataUnpickler)

    let private decodeMetadata data = decode metadataUnpickler data |> toResult mkMetadata
    
    (* Produce response *)
    let private mkTopicProducePayload (partition, errorCode, offset) : TopicProducedPayload =
      { Partition          = partition
        ErrorCode          = errorCode
        Offset             = offset }
    let private mkProduceResponsePayload (name, payloads) : ProduceResponsePayload =
      { TopicName          = name
        TopicPayload       = payloads |> List.map mkTopicProducePayload }
    let private mkProduceResponse payloads =
      payloads |> List.map mkProduceResponsePayload

    let private produceTopicUnpickler      = upTriple upInt32 upInt16 upInt64
    let private producePayloadUnpickler    = upPair upString (upList produceTopicUnpickler)
    let private produceUnpickler           = upList producePayloadUnpickler

    let private decodeProduce data = decode produceUnpickler data |> toResult mkProduceResponse
    
    (* Common *)
    let private decodeResponse<'T> data =
      match typeof<'T> with
      | t when t = typeof<MetadataResponse>     -> decodeMetadata data     |> maybe ResponseType.MetadataResponse
      | t when t = typeof<ProduceResponse>      -> decodeProduce data      |> maybe ResponseType.ProduceResponse
//      | t when t = typeof<FetchResponse>        -> decodeFetch data        |> maybe ResponseType.FetchResponse
//      | t when t = typeof<OffsetResponse>       -> decodeOffset data       |> maybe ResponseType.OffsetResponse
//      | t when t = typeof<OffsetCommitResponse> -> decodeOffsetCommit data |> maybe ResponseType.OffsetCommitResponse
//      | t when t = typeof<OffsetFetchResponse>  -> decodeOffsetFetch data  |> maybe ResponseType.OffsetFetchResponse
      | t -> sprintf "type not supported: %A" t |> failwith
    
    let private response correlationId message =
      { ResponseMessage.CorrelationId = correlationId
        ResponseMessage = message } |> RequestOrResponseType.ResponseMessage

    let private output size message : RequestOrResponse =
      { Size    = size
        Message = message }
        
    let private toAsync v = async { return v }

    let decode<'T> size correlationId data =
      decodeResponse<'T> data |> maybe (response correlationId)|> maybe (output size) |> toAsync

    let decodeInt data =
      Unpickle.decode upInt32 data |> toResult id |> toAsync

    let requestToResponseDecode request = async {
      match request.Message with
      | RequestMessage r ->
          let decode =
            match r.RequestMessage with
            | MetadataRequest     _ -> decode<MetadataResponse>
            | ProduceRequest      _ -> decode<ProduceResponse>
            | FetchRequest        _ -> decode<FetchResponse>
            | OffsetRequest       _ -> decode<OffsetResponse>
            | OffsetCommitRequest _ -> decode<OffsetCommitResponse>
            | OffsetFetchRequest  _ -> decode<OffsetFetchResponse>
          return decode |> Common.Success
      | _ -> return exn "Response in the requests" |> Common.Failure }
