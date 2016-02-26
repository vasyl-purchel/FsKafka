namespace FsKafka

open FsKafka.Common
open FsKafka.Protocol
open FsKafka.Logging

module Producer =
  type ProducerType                 = Sync | Async
  and  TopicMetadataRefreshInterval = OnlyOnError | AfterEveryMessage | Scheduled of int
  and  Compression                  = None | GZip | Snappy
  and  Codec<'T>                    = 'T -> byte[]
  and  Partitioner                  = (MetadataProvider.PartitionId * Connection.Endpoint) list -> string -> byte[] -> (MetadataProvider.PartitionId * Connection.Endpoint)
  and  Message<'T>                  = { Value: 'T; Key: byte[]; Topic: string }
  and  Config<'a>                   =
    { RequestRequiredAcks:            int16 // in documentation -1 wait for everyone to acknowledge, 0 - don't resturn response, 1 - leader acknowledge, 2+ - number of nodes to acknowledge, but with 2+ I receive error that wrong Acks (maybe something to do with test kafka configuration)
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
    { RequestRequiredAcks            = 1s
      ClientId                       = "FsKafka"
      RequestTimeoutMs               = 10000
      ProducerType                   = Sync (*not implemented*)
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

    let writeBatchToBroker attempt (endpoint, batch:(_ * int * Message<'a>) seq) = async {
      let payload =
        batch
        |> Seq.groupBy(fun (_, _, m) -> m.Topic)
        |> Seq.map toTopicPayload
        |> List.ofSeq
      let request = FsKafka.Protocol.Request.produce config.ClientId 0 config.RequestRequiredAcks config.RequestTimeoutMs payload
      let! writeResult = connection.Send(endpoint, request)
      match writeResult with
      | Success r -> verbosef (fun f -> f "Batch sent to Host=%s, Port=%i, BatchSize=%i, Response=%A" endpoint.Host endpoint.Port (batch |> Seq.length) r)
      | Failure e -> verbosee e (sprintf "Batch failed on Host=%s, Port=%i, BatchSize=%i" endpoint.Host endpoint.Port (batch |> Seq.length)) (* need to retry *) }
      (* Retry logic:
           - log failed attempt
           - wait RetryBackoffMs
           - refresh metadata
           - if attempt < conf.MessageSendMaxRetries then
               - regroup batch if needed
               - retry it with (attempt+1)
             else log max fail attempts reached *)

    let addBrokerData message =
      try
        match metadata.BrokersFor message.Topic |> Async.RunSynchronously with
        | Success (_, brokers) ->
            let (MetadataProvider.PartitionId partitionId, endpoint) = config.Partitioner brokers message.Topic message.Key
            Some(endpoint, partitionId, message)
        | Failure err          ->
            verbosee err (sprintf "Failed to retrieve brokers for Topic=%s" message.Topic )
            Option.None
      with exn -> errorEvent.Trigger(exn); Option.None

    let writeBatch (batch:Message<'a> seq) =
      let data = batch |> Seq.map addBrokerData
      if data |> Seq.exists Option.isNone |> not
      then
        data
        |> Seq.map Option.get
        |> Seq.groupBy (fun (endpoint, _, _) -> endpoint)
        |> Seq.map (writeBatchToBroker 0)
        |> Async.Parallel
        |> Async.Ignore
      else async {()}
      

    do
      verbosef (fun f -> f "initializing producer")
      match errorHandler with
      | Some handler -> errorEvent.Publish.Add(handler)
      | Option.None  -> errorEvent.Publish.Add(defaultErrorHandler)
      batchAgent.BatchProduced.Add (writeBatch >> Async.Start)

    member x.Send(topic, key, message) = batchAgent.Enqueue { Topic = topic; Value = message; Key = key }

  let start<'a> (config:Config<'a>) connection metadataProvider = T<'a>(config, connection, metadataProvider)

  let send (producer:T<_>) topic key message = producer.Send(topic, key, message)
