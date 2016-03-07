namespace FsKafka

open FsKafka.Common
open FsKafka.Protocol
open FsKafka.Logging
open System
open System.Threading
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

  type T(config:Config, connection:Connection.T) =
    let verbosef f = verbosef config.Log "FsKafka.MetadataProvider" f
    let failWith e = fatale config.Log "FsKafka.MetadataProvider" e ""; raise e
    
    let locker = obj()
    let metadata = new Dictionary<TopicName, Dictionary<PartitionId, Connection.Endpoint>>()
    
    let rec validateBrokers  acc = function
      | []    -> acc
      | x::xs ->
          match (x.NodeId, x.Host, x.Port) with
          | (-1, _, _)                             -> validateBrokers false xs
          | ( _, h, _) when String.IsNullOrEmpty h -> BrokerHostMissingException() |> failWith
          | ( _, _, p) when p <= 0                 -> BrokerPortMissingException() |> failWith
          | _                                      -> validateBrokers acc xs
      
    let rec validateMetadata acc = function
      | []    -> acc
      | x::xs ->
          match x.TopicErrorCode with
          | c when c = int16 ErrorResponseCode.NoError                             -> validateMetadata acc xs
          | c when c = int16 ErrorResponseCode.LeaderNotAvailable                  -> validateMetadata false xs
          | c when c = int16 ErrorResponseCode.OffsetsLoadInProgressCode           -> validateMetadata false xs
          | c when c = int16 ErrorResponseCode.ConsumerCoordinatorNotAvailableCode -> validateMetadata false xs
          | c                                        -> TopicErrorReceivedException(x.TopicName, c) |> failWith

    let updateBrokers brokers    =
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
    
    let rec refreshMetadataLoop request attempt =
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
                Thread.Sleep (attempt * attempt * config.RetryBackoffMs)
                refreshMetadataLoop request (attempt + 1)
          | _  -> sprintf "received not metadataResponse: %A" r |> UnexpectedException |> failWith
      | None   -> ServerUnreachableException() |> failWith

    let refreshMetadata newTopics =
      let currentTopics = metadata.Keys |> Set.ofSeq |> Set.map (fun (TopicName s) -> s)
      let topics = newTopics + currentTopics |> Set.toList
      let request = Request.metadata config.ClientId topics
      refreshMetadataLoop request 0

    let toMetadata topic =
      lock (locker) ( fun _ ->
        if not (metadata.ContainsKey (TopicName topic)) then [topic] |> Set.ofList |> refreshMetadata
        let result = metadata.[topic |> TopicName] |> Seq.map keyValueToTuple |> List.ofSeq
        Success(topic, result) )
    
    let refreshMetadataForTopics topics = lock(locker) (fun _ -> topics |> Set.ofList |> refreshMetadata)

    do
      if not config.TestConnectionTopics.IsEmpty
      then refreshMetadataForTopics config.TestConnectionTopics

    member x.RefreshMetadata topics = refreshMetadataForTopics topics
    member x.BrokersFor      topic  = topic  |> toMetadata

  let  create config connection = T(config, connection)