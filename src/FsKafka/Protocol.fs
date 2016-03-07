namespace FsKafka

module Protocol =
  (* For protocol description please refer to https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol *)
  
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
    | StaleLeaderEpochCode                = 13
    | OffsetsLoadInProgressCode           = 14
    | ConsumerCoordinatorNotAvailableCode = 15
    | NotCoordinatorForConsumerCode       = 16
    
  type MessageCodec =
    | None   = 0x00
    | GZIP   = 0x01
    | Snappy = 0x02
  type Message =
    { Crc:                int32
      MagicByte:          int8
      Attributes:         MessageCodec
      Key:                byte[]
      Value:              byte[] }
  type MessageSetEntry =
    { Offset:             int64
      MessageSize:        int32
      Message:            Message }
  type MessageSet = MessageSetEntry list

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
        - offset fetch *)

    let private apiVersion         = 0s
    let private messageVersion     = int8 0
    let private encodeTimeCreation = 0
    
    let requestWithCorrelator correlator (message:RequestMessage) =
      { RequestOrResponse.Size = encodeTimeCreation
        Message = { message with CorrelationId = correlator } |> RequestMessage }

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

    let private requestMessage clientId message =
      { ApiKey         = message |> toApiKey
        ApiVersion     = apiVersion
        CorrelationId  = encodeTimeCreation
        ClientId       = clientId
        RequestMessage = message }

    let metadata clientId topics =
      { MetadataRequest.TopicName = topics }
      |> RequestType.MetadataRequest
      |> requestMessage clientId

    // messages = (Key * Value) list
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
    let produce clientId acks timeout payload =
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
      |> requestMessage clientId
      
    open Pickle

    let stub = Pickle.pUnit [||]

    let private encodeMetadataRequest (r:MetadataRequest) =
      pList pString r.TopicName

    let private encodeMessage (m:Message) =
      let messageDataPickler (m:Message) =
        let messageValue =
          match m.Attributes with
          | MessageCodec.None   -> m.Value
          | MessageCodec.GZIP   -> Compression.gzipCompress m.Value
          | MessageCodec.Snappy -> Compression.snappyCompress m.Value
          | c                   -> sprintf "Unsupported compression %A" c |> failwith
        (pInt8 m.MagicByte) >> (pInt8 (m.Attributes |> int8)) >> (pBytes m.Key) >> (pBytes messageValue)
        
      let messageData = encode messageDataPickler m
      let crc = Crc32.calculate messageData |> int32
      (pInt32 crc) >> (pUnit messageData)

    let private encodeMessageSetEntry (e:MessageSetEntry) =
      (pInt64 e.Offset) >> (pInt32 e.MessageSize) >> (encodeMessage e.Message)

    let private encodeMessageSet v =
      let f v s = List.fold(fun s e -> encodeMessageSetEntry e s) s v
      f v

    let private encodeProduceTopicPayload (p:ProduceTopicPayload) =
      (pInt32 p.Partition) >> (pInt32 p.MessageSet.Length) >> (encodeMessageSet p.MessageSet)

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

    let compress (codec:MessageCodec) (s:MessageSet) =
      let wrap () : MessageSet =
        let message =
          { Crc         = encodeTimeCreation
            MagicByte   = messageVersion
            Attributes  = codec
            Key         = [||]
            Value       = s |> encode encodeMessageSet }
        [ { Offset      = 0L
            MessageSize = encodeTimeCreation
            Message     = message } ]
      match codec with
      | MessageCodec.None   -> s
      | MessageCodec.GZIP   -> wrap()
      | MessageCodec.Snappy -> wrap()
      | c                   -> sprintf "Unsupported compression %A" c |> failwith

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
//
//    let inline private toResult = function
//      | Failure err       -> sprintf "%A" err |> exn |> Common.Failure
//      | Success (data, _) -> data |> Common.Success
//    
    (* Metadata response *)
    let private brokerUnpickler stream = unpickle {
      let! (nodeId, stream) = upInt32  stream
      let! (host,   stream) = upString stream
      let! (port,   stream) = upInt32  stream
      return { NodeId = nodeId
               Host   = host
               Port   = port }, stream }
    let private partitionMetadataUnpickler stream = unpickle {
      let! (errorCode,   stream) = upInt16        stream
      let! (partitionId, stream) = upInt32        stream
      let! (leader,      stream) = upInt32        stream
      let! (replicas,    stream) = upList upInt32 stream
      let! (isr,         stream) = upList upInt32 stream
      return { PartitionErrorCode = errorCode
               PartitionId        = partitionId
               Leader             = leader
               Replicas           = replicas
               Isr                = isr }, stream }
    let private topicMetadataUnpickler stream = unpickle {
      let! (errorCode,  stream) = upInt16                           stream
      let! (name,       stream) = upString                          stream
      let! (partitions, stream) = upList partitionMetadataUnpickler stream
      return { TopicErrorCode     = errorCode
               TopicName          = name
               PartitionMetadata  = partitions }, stream }
    let private metadataUnpickler stream = unpickle {
      let! (brokers, stream) = upList brokerUnpickler        stream
      let! (topics,  stream) = upList topicMetadataUnpickler stream
      return { Broker        = brokers
               TopicMetadata = topics }, stream }

    let private decodeMetadata data = decode metadataUnpickler data |> Result.map fst
    
    (* Produce response *)
    let private produceTopicUnpickler stream = unpickle {
      let! (partition, stream) = upInt32 stream
      let! (errorCode, stream) = upInt16 stream
      let! (offset,    stream) = upInt64 stream
      return { TopicProducedPayload.Partition = partition
               TopicProducedPayload.ErrorCode = errorCode
               TopicProducedPayload.Offset    = offset }, stream }
    let private producePayloadUnpickler stream = unpickle {
      let! (name,     stream) = upString                     stream
      let! (payloads, stream) = upList produceTopicUnpickler stream
      return { ProduceResponsePayload.TopicName    = name
               ProduceResponsePayload.TopicPayload = payloads }, stream }
    let private produceUnpickler stream = unpickle {
      return! upList producePayloadUnpickler stream }

    let private decodeProduce data = decode produceUnpickler data |> Result.map fst
    
    (* Common *)
    let private decodeResponse<'T> data =
      match typeof<'T> with
      | t when t = typeof<MetadataResponse>     -> decodeMetadata data     |> Result.map ResponseType.MetadataResponse
      | t when t = typeof<ProduceResponse>      -> decodeProduce data      |> Result.map ResponseType.ProduceResponse
//      | t when t = typeof<FetchResponse>        -> decodeFetch data        |> Result.map ResponseType.FetchResponse
//      | t when t = typeof<OffsetResponse>       -> decodeOffset data       |> Result.map ResponseType.OffsetResponse
//      | t when t = typeof<OffsetCommitResponse> -> decodeOffsetCommit data |> Result.map ResponseType.OffsetCommitResponse
//      | t when t = typeof<OffsetFetchResponse>  -> decodeOffsetFetch data  |> Result.map ResponseType.OffsetFetchResponse
      | t -> sprintf "type not supported: %A" t |> failwith
    
    let private response correlationId message =
      { CorrelationId   = correlationId
        ResponseMessage = message }

    let decode<'T> correlationId data =
      decodeResponse<'T> data |> Result.map (response correlationId)

    let decodeInt data =
      Unpickle.decode upInt32 data |> Result.map fst

    let decoderFor = function
      | MetadataRequest     _ -> decode<MetadataResponse>     |> Result.Success
      | ProduceRequest      _ -> decode<ProduceResponse>      |> Result.Success
      | FetchRequest        _ -> decode<FetchResponse>        |> Result.Success
      | OffsetRequest       _ -> decode<OffsetResponse>       |> Result.Success
      | OffsetCommitRequest _ -> decode<OffsetCommitResponse> |> Result.Success
      | OffsetFetchRequest  _ -> decode<OffsetFetchResponse>  |> Result.Success
      