namespace FsKafka.Tests

open NUnit.Framework
open FsUnit
open FsKafka

[<TestFixture>]
module CodecTests =
  type Metadata = { TopicName:string list }
  type RequestType =
      | Metadata of Metadata
  type RequestMessage = { ApiKey: int16; ApiVersion: int16; CorrelationId: int32; ClientId: string; RequestMessage: RequestType }
  type Request = { Size: int32; Message: RequestMessage }
  
  let makeBaseRequest apiKey correlationId requestMessage =
    { Size = 0
      Message = { ApiKey = apiKey
                  ApiVersion = 0s
                  CorrelationId = correlationId
                  ClientId = "FsSharp"
                  RequestMessage = requestMessage } }
  let makeMetadataRequest id topics =
    makeBaseRequest 3s id (RequestType.Metadata {TopicName = topics})
  let requestMessageToTuple (r:RequestMessage) = (r.ApiKey, r.ApiVersion, r.CorrelationId, r.ClientId, r.RequestMessage)
  
  let requestType (r:RequestType) =
    match r with
    | Metadata metadata -> Pickle.pList Pickle.pString metadata.TopicName
  let requestMessage (r:RequestMessage) =
    (Pickle.pInt16 r.ApiKey) >> (Pickle.pInt16 r.ApiVersion) >> (Pickle.pInt32 r.CorrelationId) >> (Pickle.pString r.ClientId) >> (requestType r.RequestMessage)
  let encoder (r:Request) =
    let data = Pickle.encode requestMessage r.Message
    (Pickle.pInt32 data.Length) >> (Pickle.pUnit data)
  
  let expectedResult =
    [| 0uy; 0uy; 0uy; 44uy; 0uy; 3uy; 0uy; 0uy; 0uy; 0uy; 0uy; 1uy; 0uy; 7uy; 70uy;
       115uy; 83uy; 104uy; 97uy; 114uy; 112uy; 0uy; 0uy; 0uy; 2uy; 0uy; 9uy; 84uy;
       101uy; 115uy; 116uy; 84uy; 111uy; 112uy; 105uy; 99uy; 0uy; 10uy; 84uy; 101uy;
       115uy; 116uy; 84uy; 111uy; 112uy; 105uy; 99uy; 50uy |]

  [<Test>]
  let ``Encode metadata request should produce correct byte representation`` () =
    makeMetadataRequest 1 ["TestTopic"; "TestTopic2"]
    |> Pickle.encode encoder
    |> should equal expectedResult

  type Response = { ApiKey: int16;  ApiVersion: int16; CorrelationId: string; RequestMessage: Metadata }
  
  let makeResp (apiKey, apiVersion, correlationId, topics) = {ApiKey = apiKey; ApiVersion = apiVersion; CorrelationId = correlationId; RequestMessage = { TopicName = topics } }
  let respEncoder (r:Response) = (Pickle.pInt16 r.ApiKey) >> (Pickle.pInt16 r.ApiVersion) >> (Pickle.pString r.CorrelationId) >> (Pickle.pList Pickle.pString r.RequestMessage.TopicName)
  let decoder = Unpickle.upQuadruple Unpickle.upInt16 Unpickle.upInt16 Unpickle.upString (Unpickle.upList Unpickle.upString)
  
  [<Test>]
  let ``Decode metadata response should produce correct instance`` () =
    let resp = {ApiKey = 3s; ApiVersion = 0s; CorrelationId = "FsKafka"; RequestMessage = { TopicName = ["TestTopic"; "TestTopic2"]}}
    let encodedResp = Pickle.encode respEncoder resp
    Unpickle.decode decoder encodedResp
    |> function
       | Unpickle.Success((r), _) -> makeResp r
       | Unpickle.Failure err     -> failwith (sprintf "expected Unpickle.Success but for Failure: %A" err)
    |> should equal resp
