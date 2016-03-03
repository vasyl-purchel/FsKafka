namespace FsKafka
// https://kafka.apache.org/08/configuration.html
open FsKafka.Common
open FsKafka.Protocol
open FsKafka.Logging

module Producer =
  type ProducerType                 = Sync | Async
  type TopicMetadataRefreshInterval = OnlyOnError | AfterEveryMessage | Scheduled of int
  type RequestRequiredAcks          = None | Leader | All | Special of int16
  let acksToInt16 = function
    | None      -> 0s
    | Leader    -> 1s
    | All       -> -1s
    | Special x -> if x > 1s then x else sprintf "Wrong RequestRequiredAcks: Special(%A)" x |> failwith
  type Compression                  = None | GZip | Snappy
  type Codec<'T>                    = 'T -> byte[]
  type Partitioner                  = (MetadataProvider.PartitionId * Connection.Endpoint) list -> string -> byte[] -> (MetadataProvider.PartitionId * Connection.Endpoint)
  type Message<'T>                  = { Value: 'T; Key: byte[]; Topic: string }
  type Config<'a>                   =
    { RequestRequiredAcks:            RequestRequiredAcks
      ClientId:                       string
      RequestTimeoutMs:               int
      ProducerType:                   ProducerType
      Codec:                          Codec<'a>
      Partitioner:                    Partitioner
      Compression:                    Compression
      CompressedTopics:               string list
      MessageSendMaxRetries:          int
      RetryBackoffMs:                 int
      TopicMetadataRefreshIntervalMs: TopicMetadataRefreshInterval
      BatchBufferingMaxMs:            int
      BatchNumberMessages:            int
      SendBufferBytes:                int
      Log:                            Logger }

  let defaultConfig codec =
    { RequestRequiredAcks            = Leader
      ClientId                       = "FsKafka"
      RequestTimeoutMs               = 10000
      ProducerType                   = Sync
      Codec                          = codec
      Partitioner                    = fun partitions topic key -> System.Linq.Enumerable.First(partitions)
      Compression                    = Compression.None (*not implemented*)
      CompressedTopics               = [] (*not implemented*)
      MessageSendMaxRetries          = 3 (*not implemented*)
      RetryBackoffMs                 = 100 (*not implemented*)
      TopicMetadataRefreshIntervalMs = OnlyOnError (*not implemented*)
      BatchBufferingMaxMs            = 5000
      BatchNumberMessages            = 10000
      SendBufferBytes                = 100 * 1024 (*not implemented*)
      Log                            = defaultLogger Verbose }
  
  type T<'a>(config:Config<'a>, connection:Connection.T, metadata:MetadataProvider.T, ?errorHandler:exn -> unit) =
    let verbosef f = verbosef config.Log "FsKafka.Producer" f
    let verbosee   = verbosee config.Log "FsKafka.Producer"

    let defaultErrorHandler (exn:exn) : unit =
      verbosee exn "Some fatal exception happened"
      exit(1)

    let errorEvent = new Event<exn>()

    let batchAgent = BatchAgent<Message<'a>>(config.BatchNumberMessages, config.BatchBufferingMaxMs)
    
    let toMessageSet (partition, messages) =
      let messages =
        messages
        |> Seq.map(fun (_,_,m) -> m.Key, m.Value |> config.Codec)
        |> List.ofSeq
      partition, FsKafka.Protocol.Request.mkMessageSet FsKafka.Protocol.MessageCodec.None messages

    let toTopicPayload (topic, messages) =
      let payload =
        messages
        |> Seq.groupBy (fun (_,p,_) -> p)
        |> Seq.map toMessageSet
        |> List.ofSeq
      topic, payload

    let readResponse = config.RequestRequiredAcks <> RequestRequiredAcks.None

    let produceRequest messages =
      messages
      |> Seq.groupBy(fun (_, _, m) -> m.Topic)
      |> Seq.map toTopicPayload
      |> List.ofSeq
      |> Request.produce config.ClientId (config.RequestRequiredAcks |> acksToInt16) config.RequestTimeoutMs

    let rec writeBatchToBroker attempt (endpoint, batch:(_ * int * Message<'a>) seq) = async {
      let request = produceRequest batch
      let! writeResult = connection.SendAsync(endpoint, request, readResponse)
      match writeResult with
      | Success r -> verbosef (fun f -> f "Batch sent to Host=%s, Port=%i, BatchSize=%i, Response=%A" endpoint.Host endpoint.Port (batch |> Seq.length) r)
      | Failure e ->
          verbosee e (sprintf "Batch failed on Host=%s, Port=%i, BatchSize=%i" endpoint.Host endpoint.Port (batch |> Seq.length))
          match e with
          | :? Connection.FailedAddingRequestException -> () (*safe to retry*)
          (*write result failed*)
          (*response await failed*)
          (*response contains errorCodes...*) }
      (* Retry logic:
           - log failed attempt
           - wait RetryBackoffMs
           - refresh metadata
           - if attempt < conf.MessageSendMaxRetries then
               - regroup batch if needed
               - retry it with (attempt+1)
             else log max fail attempts reached *)
             
    let rec syncWriteBatchToBroker attempt (endpoint, batch:(_ * int * Message<'a>) seq) =
      let request = produceRequest batch
      let writeResult = connection.Send(endpoint, request, readResponse)
      match writeResult with
      | Success r -> verbosef (fun f -> f "Batch sent to Host=%s, Port=%i, BatchSize=%i, Response=%A" endpoint.Host endpoint.Port (batch |> Seq.length) r)
      | Failure e ->
          verbosee e (sprintf "Batch failed on Host=%s, Port=%i, BatchSize=%i" endpoint.Host endpoint.Port (batch |> Seq.length))
          match e with
          | :? Connection.FailedAddingRequestException -> ()
          | _ -> ()

    let addBrokerData message =
      try
        match metadata.BrokersFor message.Topic with
        | Success (_, brokers) ->
            let (MetadataProvider.PartitionId partitionId, endpoint) = config.Partitioner brokers message.Topic message.Key
            Some(endpoint, partitionId, message)
        | Failure err          ->
            verbosee err (sprintf "Failed to retrieve brokers for Topic=%s" message.Topic )
            Option.None
      with exn -> errorEvent.Trigger(exn); Option.None

    let writeBatch (batch:Message<'a> seq) =
      let data = batch |> Seq.map addBrokerData
      if data |> Seq.exists Option.isNone then async {()}
      else
        data
        |> Seq.map Option.get
        |> Seq.groupBy (fun (endpoint, _, _) -> endpoint)
        |> Seq.map (writeBatchToBroker 0)
        |> Async.Parallel
        |> Async.Ignore
        
    let syncSend (batch:Message<'a> seq) =
      let data = batch |> Seq.map addBrokerData
      if data |> Seq.exists Option.isNone then ()
      else
        data
        |> Seq.map Option.get
        |> Seq.groupBy (fun (endpoint, _, _) -> endpoint)
        |> Seq.iter (syncWriteBatchToBroker 0)

    do
      verbosef (fun f -> f "initializing producer")
      match errorHandler with
      | Some handler -> errorEvent.Publish.Add(handler)
      | Option.None  -> errorEvent.Publish.Add(defaultErrorHandler)
      batchAgent.BatchProduced.Add (writeBatch >> Async.Start)

    member x.Send(topic, key, message) =
      match config.ProducerType with
      | Async -> batchAgent.Enqueue { Topic = topic; Value = message; Key = key }
      | Sync  -> syncSend [ { Topic = topic; Value = message; Key = key } ]

    member x.Send(messages:Message<'a> list) =
      match config.ProducerType with
      | Async -> messages |> List.iter batchAgent.Enqueue
      | Sync  -> messages |> syncSend

  let start<'a> (config:Config<'a>) connection metadataProvider = T<'a>(config, connection, metadataProvider)

  let send (producer:T<_>) topic key message = producer.Send(topic, key, message)
