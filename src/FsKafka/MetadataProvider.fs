namespace FsKafka

open FsKafka.Common
open FsKafka.Protocol
open FsKafka.Logging
open System
open System.Collections.Generic

module MetadataProvider =
  type Config =
    { TestConnectionTopics: string list
      Log:                  Logger
      ClientId:             string
      RetryBackoffMs:       int }
  let defaultConfig =
    { TestConnectionTopics = []
      Log                  = defaultLogger LogLevel.Verbose
      ClientId             = "FsKafka"
      RetryBackoffMs       = 100 }

  type TopicName     = TopicName of string
  type PartitionId   = PartitionId of int
  type TripleWay<'a> = Success of 'a | Error of exn | Retry

  let toTriple errorMsg = function
    | Some v -> Success v
    | None   -> exn errorMsg |> Error

  let isError = function
    | Error _ -> true
    | _       -> false

  let isRetry = function
    | Retry -> true
    | _     -> false

  type T(config:Config, connection:Connection.T) =
    let verbosef f = verbosef config.Log "FsKafka.MetadataProvider" f
    
    let metadata = new Dictionary<TopicName, Dictionary<PartitionId, Connection.Endpoint>>()
    
    let checkpoint         = AsyncCheckpoint()
    let ensureSingleThread = ensureSingleThread()

    let validateBroker broker =
      match broker.NodeId, broker.Host, broker.Port with
      | (-1, _, _)                                  -> Retry
      | ( _, s, _) when String.IsNullOrWhiteSpace s -> exn "Broker missing host information." |> Error
      | ( _, _, p) when p <= 0                      -> exn "Broker missing port information." |> Error
      | _                                           -> Success ()
      
    let validateMetadata (metadata:TopicMetadata) =
      match metadata.TopicErrorCode with
      | c when c = int16 ErrorResponseCode.NoError                             -> Success ()
      | c when c = int16 ErrorResponseCode.LeaderNotAvailable                  -> Retry
      | c when c = int16 ErrorResponseCode.OffsetsLoadInProgressCode           -> Retry
      | c when c = int16 ErrorResponseCode.ConsumerCoordinatorNotAvailableCode -> Retry
      | c                                                                      -> sprintf "Topic:%s returned error %A" metadata.TopicName c |> exn |> Error

    let refreshMetadata newTopics =
      let currentTopics = metadata.Keys |> Set.ofSeq |> Set.map (fun (TopicName s) -> s)
      let topics = newTopics + currentTopics |> Set.toList

      let map f = function
        | Some v -> f v
        | None   -> None

      let rec loop request attempt = async {
        let refreshResult = connection.TryPick request |> map Optics.getMetadataResponse
        match refreshResult with
        | Some r ->
            verbosef (fun f -> f "Received metadata response: %A" r)
            let validations = List.append (r.Broker |> List.map validateBroker) (r.TopicMetadata |> List.map validateMetadata)
            if validations |> List.exists isError then failwith "error received" // ??? probably should terminate the service...
            if validations |> List.exists isRetry then
              do! Async.Sleep (attempt * attempt * config.RetryBackoffMs)
              return! loop request (attempt + 1)
            else
              r.Broker
              |> List.map (fun b -> {Connection.Endpoint.Host = b.Host; Connection.Endpoint.Port = b.Port})
              |> Set.ofList 
              |> connection.UpdateBrokers

              metadata.Clear()
              r.TopicMetadata
              |> List.iter(fun topic ->
                  let partitions = new Dictionary<PartitionId, Connection.Endpoint>()
                  topic.PartitionMetadata
                  |> List.iter ( fun partition ->
                      let broker = r.Broker |> List.find(fun b -> b.NodeId = partition.Leader)
                      let endpoint = { Connection.Endpoint.Host = broker.Host; Connection.Endpoint.Port = broker.Port }
                      partitions.Add(partition.PartitionId |> PartitionId, endpoint) )
                  metadata.Add(topic.TopicName |> TopicName, partitions) )
        | None   -> failwith "server unreachable" }

      let request = FsKafka.Protocol.Request.metadata config.ClientId 0 topics
      ensureSingleThread (checkpoint.WithClosedDoors (loop request 0))

    let toMetadata topic =
      checkpoint.OnPassage (async {
        if not (metadata.ContainsKey (TopicName topic))
        then do! [topic] |> Set.ofList |> refreshMetadata
        let result = metadata.[topic |> TopicName] |> Seq.map keyValueToTuple |> List.ofSeq
        return Result.Success(topic, result) })

    do
      if not config.TestConnectionTopics.IsEmpty then
        config.TestConnectionTopics |> Set.ofList |> refreshMetadata |> Async.Start
      else ensureSingleThread (checkpoint.WithClosedDoors (async { verbosef (fun f -> f "opening doors for metadata") } ) ) |> Async.Start

    member x.RefreshMetadata topics = topics |> Set.ofList |> refreshMetadata
    member x.BrokersFor      topic  = topic  |> toMetadata

  let create config connection = T(config, connection)