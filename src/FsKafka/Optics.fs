[<RequireQualifiedAccess>]
module FsKafka.Optics

open FsKafka.Common
open FsKafka.Pickle
open FsKafka.Protocol.Common
open FsKafka.Protocol.Requests
open FsKafka.Protocol.Responses
open FsKafka.Unpickle

let private apiVersion     = 1s //0s - checked, maybe need to be able to change
let private messageVersion = 1y //0y - checked, probably 1y is for kafka 0.10.0

(* ========= Optics ========= *)

let withCorrelator correlator message : RequestMessage =
  { message with CorrelationId = correlator }

(* ========= Requests creators ========= *)

let private requestMessage clientId message =
  RequestMessage.Create(message |> toApiKey, apiVersion, 0, clientId, message)

// messages = (Key * Value) seq
let mkMessageSet codec messages : MessageSet =
  let toMessageSetItem (key, value) =
    Message.Create(messageVersion, codec, key, value)
    |> MessageSetItem.Create
  messages
  |> Seq.map toMessageSetItem
  |> MessageSet.Create

let metadata clientId topics =
  MetadataRequest.Create(topics)
  |> RequestType.Metadata
  |> requestMessage clientId
      
let groupCoordinator clientId groupId =
  GroupCoordinatorRequest.Create(groupId)
  |> RequestType.GroupCoordinator
  |> requestMessage clientId
      
let joinGroup clientId groupId timeout memberId protocol protocols =
  JoinGroupRequest.Create(groupId, timeout, memberId, protocol, protocols)
  |> RequestType.JoinGroup
  |> requestMessage clientId

// data = (topic * ( (partitionId * MessageSetBytes) list ) ) list
let produce clientId acks timeout data =
  let mapTopicPartitionData (topic, data) =
    let data = data |> List.map TopicPartitionData.Create
    TopicData.Create(topic, data)
  let data = data |> List.map mapTopicPartitionData
  ProduceRequest.Create(acks, timeout, data)
  |> RequestType.Produce
  |> requestMessage clientId
  
let private wrapMessageSet codec s =
  let data = FsKafka.Pickle.encode MessageSet.Pickler s
  let message = Message.Create(messageVersion, codec, [||], data)
  seq [ MessageSetItem.Create(message) ] |> MessageSet.Create

(* ========= Codecs ========= *)
          
let encodeMessageSet codec (messageSet:MessageSet) =
  match codec with
  | MessageCodec.None   -> messageSet
  | c                   -> wrapMessageSet c messageSet
  |> FsKafka.Pickle.encode MessageSet.Pickler

let private sizePickler (data:byte[]) =
  fun _ -> (pInt32 data.Length) >> (pUnit data)

let encode (message:RequestMessage) =
  let data = FsKafka.Pickle.encode RequestMessage.Pickler message
  FsKafka.Pickle.encode (sizePickler data) message
  
let decodeInt data =
  FsKafka.Unpickle.decode FsKafka.Unpickle.upInt32 data
  |> Result.map fst
      
let decode request correlationId data =
  let toResponseMessage message =
    ResponseMessage.Create(correlationId, message) 
  let decode unpickler toResponseTypeF =
    decode unpickler data |> Result.map (fst >> toResponseTypeF >> toResponseMessage )
  match request with
  | RequestType.Produce            _ -> decode ProduceResponse.Unpickler            Produce
  //| RequestType.Fetch              _ -> decode FetchResponse.Unpickler              Fetch
  //| RequestType.Offsets            _ -> decode OffsetsResponse.Unpickler            Offsets
  | RequestType.Metadata           _ -> decode MetadataResponse.Unpickler           Metadata
  //| RequestType.LeaderAndIsr       _ -> decode LeaderAndIsrResponse.Unpickler       LeaderAndIsr
  //| RequestType.StopReplica        _ -> decode StopReplicaResponse.Unpickler        StopReplica
  //| RequestType.UpdateMetadata     _ -> decode UpdateMetadataResponse.Unpickler     UpdateMetadata
  //| RequestType.ControlledShutdown _ -> decode ControlledShutdownResponse.Unpickler ControlledShutdown
  //| RequestType.OffsetCommit       _ -> decode OffsetCommitResponse.Unpickler       OffsetCommit
  //| RequestType.OffsetFetch        _ -> decode OffsetFetchResponse.Unpickler        OffsetFetch
  | RequestType.GroupCoordinator   _ -> decode GroupCoordinatorResponse.Unpickler   GroupCoordinator
  | RequestType.JoinGroup          _ -> decode JoinGroupResponse.Unpickler          JoinGroup
  //| RequestType.Heartbeat          _ -> decode HeartbeatResponse.Unpickler          Heartbeat
  //| RequestType.LeaveGroup         _ -> decode LeaveGroupResponse.Unpickler         LeaveGroup
  //| RequestType.SyncGroup          _ -> decode SyncGroupResponse.Unpickler          SyncGroup
  //| RequestType.DescribeGroups     _ -> decode DescribeGroupsResponse.Unpickler     DescribeGroups
  //| RequestType.ListGroups         _ -> decode ListGroupsResponse.Unpickler         ListGroups