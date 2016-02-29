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

  type TopicName   = TopicName of string
  type PartitionId = PartitionId of int

  exception BrokerHostMissingException  of unit
  exception BrokerPortMissingException  of unit
  exception TopicErrorReceivedException of string * int16
  exception ServerUnreachableException  of unit
  exception UnexpectedException         of string

  type T(config:Config, connection:Connection.T, ?errorHandler:exn -> unit) =
    let verbosef f = verbosef config.Log "FsKafka.MetadataProvider" f
    let fatale     = fatale   config.Log "FsKafka.MetadataProvider"
    
    let defaultErrorHandler (exn:exn) : unit =
      fatale exn ""
      exit(1)

    let errorEvent = new Event<exn>()

    let metadata = new Dictionary<TopicName, Dictionary<PartitionId, Connection.Endpoint>>()
    
    let checkpoint         = AsyncCheckpoint()
    let ensureSingleThread = ensureSingleThread()

    let rec validateBrokers acc = function
      | []    -> acc
      | x::xs ->
          match (x.NodeId, x.Host, x.Port) with
          | (-1, _, _)                             -> validateBrokers false xs
          | ( _, h, _) when String.IsNullOrEmpty h -> BrokerHostMissingException() |> errorEvent.Trigger; false
          | ( _, _, p) when p <= 0                 -> BrokerPortMissingException() |> errorEvent.Trigger; false
          | _                                      -> validateBrokers acc xs
      
    let rec validateMetadata acc = function
      | []    -> acc
      | x::xs ->
          match x.TopicErrorCode with
          | c when c = int16 ErrorResponseCode.NoError                             -> validateMetadata acc xs
          | c when c = int16 ErrorResponseCode.LeaderNotAvailable                  -> validateMetadata false xs
          | c when c = int16 ErrorResponseCode.OffsetsLoadInProgressCode           -> validateMetadata false xs
          | c when c = int16 ErrorResponseCode.ConsumerCoordinatorNotAvailableCode -> validateMetadata false xs
          | c                       -> TopicErrorReceivedException(x.TopicName, c) |> errorEvent.Trigger; false

    let updateBrokers brokers =
      brokers
      |> List.map (fun b -> {Connection.Endpoint.Host = b.Host; Connection.Endpoint.Port = b.Port})
      |> Set.ofList 
      |> connection.UpdateBrokers

    let updateMetadata nodeIdMapping newMetadata =
      metadata.Clear()
      newMetadata
      |> List.iter(fun topic ->
          let partitions = new Dictionary<PartitionId, Connection.Endpoint>()
          topic.PartitionMetadata
          |> List.iter ( fun partition ->
              let broker = nodeIdMapping partition.Leader
              let endpoint = { Connection.Endpoint.Host = broker.Host; Connection.Endpoint.Port = broker.Port }
              partitions.Add(partition.PartitionId |> PartitionId, endpoint) )
          metadata.Add(topic.TopicName |> TopicName, partitions) )

    let refreshMetadata newTopics =
      let currentTopics = metadata.Keys |> Set.ofSeq |> Set.map (fun (TopicName s) -> s)
      let topics = newTopics + currentTopics |> Set.toList

      let rec loop request attempt = async {
        let refreshResult = connection.TryPick request
        match refreshResult with
        | Some r ->
            match r.ResponseMessage with
            | MetadataResponse r ->
                verbosef (fun f -> f "Received metadata response: %A" r)
                let brokersValid  = r.Broker        |> validateBrokers true
                let metadataValid = r.TopicMetadata |> validateMetadata true
                if brokersValid && metadataValid then
                  r.Broker |> updateBrokers
                  r.TopicMetadata |> updateMetadata (fun id -> r.Broker |> List.find(fun b -> b.NodeId = id))
                else
                  do! Async.Sleep (attempt * attempt * config.RetryBackoffMs)
                  return! loop request (attempt + 1)
            | _  -> UnexpectedException( sprintf "received not metadataResponse: %A" r) |> errorEvent.Trigger
        | None   -> ServerUnreachableException() |> errorEvent.Trigger }

      let request = Request.metadata config.ClientId topics
      ensureSingleThread (checkpoint.WithClosedDoors (loop request 0))

    let toMetadata topic =
      checkpoint.OnPassage (async {
        if not (metadata.ContainsKey (TopicName topic))
        then do! [topic] |> Set.ofList |> refreshMetadata
        let result = metadata.[topic |> TopicName] |> Seq.map keyValueToTuple |> List.ofSeq
        return Result.Success(topic, result) })

    do
      match errorHandler with
      | Some handler -> errorEvent.Publish.Add(handler)
      | Option.None  -> errorEvent.Publish.Add(defaultErrorHandler)
      if not config.TestConnectionTopics.IsEmpty then
        config.TestConnectionTopics |> Set.ofList |> refreshMetadata |> Async.Start
      else ensureSingleThread (checkpoint.WithClosedDoors (async { verbosef (fun f -> f "opening doors for metadata") } ) ) |> Async.Start

    member x.RefreshMetadata topics = topics |> Set.ofList |> refreshMetadata
    member x.BrokersFor      topic  = topic  |> toMetadata

  let create config connection = T(config, connection)