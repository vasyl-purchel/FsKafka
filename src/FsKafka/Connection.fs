[<RequireQualifiedAccess>]
module FsKafka.Connection

open FsKafka.Common
open FsKafka.Protocol
open FsKafka.Logging
open FsKafka.Socket
open System
open System.Threading
open System.Threading.Tasks
open System.Collections.Concurrent
open System.Collections.Generic

type Config =
  { MetadataBrokersList:  (string * int) list
    Log:                  Logger
    RequestTimeoutMs:     int (* this is not the same as in Producer.Config, as this should be bigger then that value to include time of receiving failed response also, so 2 or 3 times bigger is good *) }
let defaultConfig =
  { MetadataBrokersList  = []
    Log                  = defaultLogger LogLevel.Verbose
    RequestTimeoutMs     = 20000 }
  
exception FailedAddingRequestException of unit
exception RequestTimedOutException     of unit

type Endpoint =
  { Host:   string
    Port:   int }
        
type AsyncClient(logger          : Logger,
                 socket          : IAsyncSocket,
                 intCodec        : byte[] -> AsyncResult<int>,
                 processResponse : AsyncResult<int * int * byte[]> -> Async<unit>,
                 host            : string,
                 port            : int) =
  let verbose msg = verbosef logger "FsKafka.Connection.Client"   (fun f -> f "[%s:%i]: %s" host port msg)
  let info    msg = infof    logger "FsKafka.Connection.Client"   (fun f -> f "[%s:%i]: %s" host port msg)
  let infoe e msg = infoef   logger "FsKafka.Connection.Client" e (fun f -> f "[%s:%i]: %s" host port msg)

  let disposed           = ref 0
  let cancellationSource = new CancellationTokenSource()
  let checkpoint         = AsyncCheckpoint()

  let cleanUp () =
    verbose "cleaning resources"
    if Interlocked.Increment disposed = 1 then
      cancellationSource.Cancel()
      checkpoint.Cancel()
      socket.Close()

  let connect =
    let logOk    () = info "connection established"
    let logError e  = infoe e "connection attempt failed, reconnecting..."
    connectAsync (socket:IAsyncSocket) logOk logError (host, port, cancellationSource.Token) 500
  
  let read (size, token) = socket.ReadAsync(size, token)                     |> checkpoint.OnPassage
  let write data         = socket.WriteAsync(data, cancellationSource.Token) |> checkpoint.OnPassage

  let reader = readAgent read intCodec cancellationSource.Token processResponse

  do Async.Start(checkpoint.WithClosedDoors connect, cancellationSource.Token)

  member x.WriteAsync      data = write data
  member x.RequestResponse ()   = reader.Post()
  member x.Close           ()   = cleanUp ()

  override x.Finalize () = cleanUp()
  interface IDisposable with
    member x.Dispose  () = cleanUp()

type SyncClient(logger   : Logger,
                socket   : ISyncSocket,
                intCodec : byte[] -> Result<int>,
                host     : string,
                port     : int) =
  let verbose msg = verbosef logger "FsKafka.Connection.Client"   (fun f -> f "[%s:%i]: %s" host port msg)
  let info    msg = infof    logger "FsKafka.Connection.Client"   (fun f -> f "[%s:%i]: %s" host port msg)
  let infoe e msg = infoef   logger "FsKafka.Connection.Client" e (fun f -> f "[%s:%i]: %s" host port msg)

  let disposed           = ref 0
  let mutable connected  = false
  let locker             = obj ()

  let cleanUp () =
    verbose "cleaning resources"
    if Interlocked.Increment disposed = 1 then
      socket.Close()

  let connect () =
    let onSuccess() = info "connection established"; connected <- true
    let logError e  = infoe e "connection attempt failed, reconnecting..."
    connectSync (socket:ISyncSocket) onSuccess logError (host, port) 500

  let request data readResponse =
    if not connected then connect ()
    Result.result {
      let! _ = socket.Write data
      if not readResponse then return None
      else
        let! sizeBytes       = socket.Read 4
        let! size            = intCodec sizeBytes
        let! correlatorBytes = socket.Read 4
        let! correlator      = intCodec correlatorBytes
        let! messageData     = socket.Read (size - 4)
        return Some(size, correlator, messageData) }
  
  member x.Send  (data, readResponse) = lock locker (fun _ -> request data readResponse)
  member x.Close ()                   = cleanUp()

  override x.Finalize () = cleanUp()
  interface IDisposable with
    member x.Dispose  () = cleanUp()

type DubleClient = { SyncClient : Lazy<SyncClient>; AsyncClient : Lazy<AsyncClient> }
// AsyncConnection...
type T(config:Config, asyncSocket: unit -> IAsyncSocket, syncSocket: unit -> ISyncSocket) =
    let verbosef f = verbosef config.Log "FsKafka.Connection" f
    let verbosee   = verbosee config.Log "FsKafka.Connection"

    (* Requests related *)
    let correlator         = ref 0
    let nextCorrelator ()  =
      if !correlator > Int32.MaxValue - 1000
      then Interlocked.Exchange(correlator, 0) |> ignore
      Interlocked.Increment correlator

    let requests = new ConcurrentDictionary<int, RequestMessage * TaskCompletionSource<ResponseMessage>>()

    let tryRemoveRequest id = async {
      match requests.TryRemove id with
      | true, (request, source) -> return Success(request, source)
      | false, _                -> return sprintf "couldn't remove request %i" id |> exn |> Failure }

    let trySetResult id (source:TaskCompletionSource<_>) result = async {
      match source.TrySetResult result with
      | true  -> return Success()
      | false -> return sprintf "couldn't set the result for %i" id |> exn |> Failure }

    let handleResponse response = asyncResult {
      let! (_, correlator, data) = response
      let! (request, source)     = tryRemoveRequest correlator
      let! decode                = Response.decoderFor request.RequestMessage |> Result.toAsync
      let! response              = decode correlator data |> Result.toAsync
      do! trySetResult correlator source response }

    let processResponse data = async {
        let! result = handleResponse data
        match result with
        | Success _ -> ()
        | Failure err -> verbosee err "failed parsing response message" }
        
    (* Brokers related *)
    let brokers = Dictionary<Endpoint, DubleClient>()
    
    let addNewBrokers =
      Set.iter (fun e ->
        let broker =
          { SyncClient  = lazy(new SyncClient (config.Log, syncSocket(),  Response.decodeInt, e.Host, e.Port))
            AsyncClient = lazy(new AsyncClient(config.Log, asyncSocket(), Response.decodeInt >> Result.toAsync, processResponse, e.Host, e.Port)) }
        brokers.Add(e, broker))

    let removeOldBrokers =
      Set.iter (fun e ->
        if brokers.[e].AsyncClient.IsValueCreated
        then brokers.[e].AsyncClient.Force().Close()
        
        if brokers.[e].SyncClient.IsValueCreated
        then brokers.[e].SyncClient.Force().Close()
        
        brokers.Remove e |> ignore )
        
    let updateBrokers validBrokers =
      let currentBrokers = brokers.Keys |> Set.ofSeq
      validBrokers - currentBrokers |> addNewBrokers
      currentBrokers - validBrokers |> removeOldBrokers
      
    (* Send and timeout related *)
    let cancelRequest (completionSource:TaskCompletionSource<_>) correlator host port =
      if requests.ContainsKey correlator then
        completionSource.TrySetCanceled() |> ignore
        requests.TryRemove correlator |> ignore
        verbosee (RequestTimedOutException()) (sprintf "Request timed out from host=%s, port=%i, CorrelationId=%i" host port correlator)
      
    let asyncSend endpoint request readResponse = async {
      let correlationId  = nextCorrelator()
      let encodedRequest = request |> Request.requestWithCorrelator correlationId |> FsKafka.Protocol.Request.encode
      let broker         = brokers.[endpoint].AsyncClient.Force()
      // save request to the requests dict
      if readResponse then
        let completionSource = new TaskCompletionSource<ResponseMessage>()
        match requests.TryAdd(correlationId, (request, completionSource)) with
        | true  ->
            let! writeResult   = broker.WriteAsync(encodedRequest)
            match writeResult with
            | Success _ ->
                verbosef (fun f -> f "Request sent: CorrelationId=%i, Message=%A" correlationId request)
                broker.RequestResponse()
                let requestCancellationToken = new CancellationTokenSource()
                requestCancellationToken.Token.Register(fun () ->
                  cancelRequest completionSource correlationId endpoint.Host endpoint.Port) |> ignore
                async {
                  do! Async.Sleep config.RequestTimeoutMs
                  requestCancellationToken.Cancel() } |> Async.Start
                let! result = Async.AwaitTask completionSource.Task
                return Some result |> Success
            | Failure e ->
                verbosee e (sprintf "Write CorrelationId=%i failed to host=%s, port=%i" correlationId endpoint.Host endpoint.Port)
                return Failure e
        | false ->
            let e = FailedAddingRequestException()
            verbosee e (sprintf "Write CorrelationId=%i failed to host=%s, port=%i" correlationId endpoint.Host endpoint.Port)
            return e |> Failure
      else
        let! writeResult = broker.WriteAsync encodedRequest
        match writeResult with
        | Success _ -> return None |> Success
        | Failure e ->
            verbosee e (sprintf "Write CorrelationId=%i failed to host=%s, port=%i" correlationId endpoint.Host endpoint.Port)
            return Failure e }
          
    let send endpoint request readResponse = Result.result {
      let correlationId  = nextCorrelator()
      let encodedRequest = request |> Request.requestWithCorrelator correlationId |> FsKafka.Protocol.Request.encode
      let broker         = brokers.[endpoint].SyncClient.Force()
      let! response      = broker.Send(encodedRequest, readResponse)
      match response with
      | Some (size, correlator, data) ->
          verbosef (fun f -> f "Request sent: CorrelationId=%i, Message=%A" correlationId request)
          let! decode                = Response.decoderFor request.RequestMessage
          let! response              = decode correlator data
          return response |> Some
      | None -> return None }
   
    let trySend request broker =
      let result = send broker request true
      match result with
      | Success(Some r) -> Some r
      | _               -> None
    
    let sendToFirstSuccessfullBroker request = brokers.Keys |> Seq.tryPick (trySend request)

    do
      config.MetadataBrokersList
      |> List.map (fun (host, port) -> { Host = host; Port = port })
      |> Set.ofList
      |> addNewBrokers

    member x.UpdateBrokers brokers                         = updateBrokers brokers
    member x.SendAsync     (broker, request, readResponse) = asyncSend broker request readResponse
    member x.Send          (broker, request, readResponse) = send broker request readResponse
    member x.TryPick       request                         = sendToFirstSuccessfullBroker request

let create config =
  match config.MetadataBrokersList with
  | [] -> failwith "empty metadata brokers list"
  | _  -> T(config, (fun _ -> Tcp.createAsync config.Log), (fun _ -> Tcp.createSync config.Log))
