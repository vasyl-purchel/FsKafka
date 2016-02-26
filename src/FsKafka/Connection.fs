namespace FsKafka

open FsKafka.Common
open FsKafka.Protocol
open FsKafka.Logging
open System
open System.Threading
open System.Threading.Tasks
open System.Net.Sockets
open System.Collections.Concurrent
open System.Collections.Generic

module Connection =

  type Config =
    { MetadataBrokersList:  (string * int) list
      Log:                  Logger
      RequestTimeoutMs:     int (* this is not the same as in Producer.Config, as this should be bigger then that value to include time of receiving failed response also, so 2 or 3 times bigger is good *) }
  let defaultConfig =
    { MetadataBrokersList  = []
      Log                  = defaultLogger LogLevel.Verbose
      RequestTimeoutMs     = 10000 }
    
  type IClient =
    abstract ConnectAsync      : string * int * CancellationToken -> Async<Result<unit>>
    abstract WriteAsync        : byte[]                           -> Async<Result<unit>>
    abstract ReadAsync         : int                              -> Async<Result<byte[]>>
    abstract Close             : unit                             -> unit

  exception SocketDisconnectedException of unit
  exception FailedAddingRequestException of unit
  exception RequestTimedOutException of unit

  type TcpIoClient (logger:Logger) =
    let verbosef f = verbosef logger "FsKafka.Connection.TcpIoClient" f
    let disposed = ref 0

    let rec readLoop (stream:NetworkStream) buffer size i = async {
      verbosef (fun f -> f "read loop size=%i, offset=%i" (size - i) i)
      let! n = stream.AsyncRead(buffer, i, size - i)
      if i + n >= size then
        verbosef (fun f -> f "read succeeded for size=%i" buffer.Length)
        return Success buffer
      else if n = 0 then
             verbosef (fun f -> f "readed 0 bytes, socket disconnected")
             return SocketDisconnectedException() |> Failure
           else
             verbosef (fun f -> f "readed readed=%i, size=%i, offset=%i" n size i)
             return! readLoop stream buffer size (i + n) }

    let client = new TcpClient()
    
    interface IClient with
      member x.ConnectAsync(host, port, token) = async {
        verbosef (fun f -> f "connecting to host=%s, port=%i" host port)
        try
          client.ConnectAsync(host = host, port = port).Wait(cancellationToken = token)
          return if client.Connected then Success()
                 else sprintf "connection failed for host=%s, port=%i" host port |> exn |> Failure
        with
        | ex -> return Failure ex }
      member x.WriteAsync   data        = 
        verbosef (fun f -> f "writing data.Length=%i" data.Length)
        data |> client.GetStream().AsyncWrite |> asyncToResult
      member x.ReadAsync    size        = 
        verbosef (fun f -> f "reading size=%i" size)
        readLoop (client.GetStream())  (Array.zeroCreate size) size 0
      member x.Close        ()          = if Interlocked.Increment disposed = 1 then (client :> IDisposable).Dispose()
    interface IDisposable with
      member x.Dispose      ()          = if Interlocked.Increment disposed = 1 then (client :> IDisposable).Dispose()

  let tcpClient logger = new TcpIoClient(logger)

  type Client(logger: Logger, client: IClient, host:string, port:int) =
    let verbosef f         = verbosef logger "FsKafka.Connection.Client" f
    let verbosee           = verbosee logger "FsKafka.Connection.Client"
    let infof f            = infof    logger "FsKafka.Connection.Client" f

    let disposed           = ref 0
    let cancellationSource = new CancellationTokenSource()
    let checkpoint         = AsyncCheckpoint(sprintf "ClientOfT[%s:%i]" host port)
    let ensureSingleThread = ensureSingleThread()

    let connect () =
      let rec loop reconnectionDelay = async {
        infof (fun f -> f "Connecting to host=%s, port=%i" host port)
        do! Async.Sleep 500
        infof (fun f -> f "Connecting started to host=%s, port=%i" host port)
        let! result = client.ConnectAsync(host, port, cancellationSource.Token)
        match result with
        | Success _ -> infof (fun f -> f "Connection established to host=%s, port=%i" host port); do! Async.Sleep 500
        | Failure e -> verbosee e (sprintf "Reconnecting after Delay=%i" reconnectionDelay)
                       do! Async.Sleep reconnectionDelay
                       return! loop (reconnectionDelay * 2) }
      
      ensureSingleThread (checkpoint.WithClosedDoors (loop 500))

    let cleanUp timeout = async {
        match timeout with
        | Some timeout -> do! Async.Sleep timeout
        | None         -> ()
        verbosef (fun f -> f "Cleaning resources on connection to host=%s, port=%i" host port)
        if Interlocked.Increment disposed = 1 then
          cancellationSource.Cancel()
          checkpoint.Cancel()
          client.Close() }

    do
      Async.Start(connect(), cancellationSource.Token)

    member x.WriteAsync data      =
      verbosef (fun f -> f "Requested WriteAsync on [%s:%i]" host port)
      checkpoint.OnPassage (async { return! client.WriteAsync data })
    member x.ReadAsync  size      =
      verbosef (fun f -> f "Requested ReadAsync on [%s:%i]" host port)
      checkpoint.OnPassage (async { return! client.ReadAsync size })
    member x.Reconnect  ()        = connect ()
    member x.Close (?timeout:int) = cleanUp timeout

    member x.CancellationToken with get () = cancellationSource.Token
    
    override x.Finalize () = cleanUp None |> Async.RunSynchronously
    interface IDisposable with
      member x.Dispose  () = cleanUp None |> Async.RunSynchronously

  type Endpoint    = { Host: string;  Port: int }
  and  Broker      = { Client: Client; Reader: MailboxProcessor<unit>}
  
  let readAgent (client : Client, cancellationToken, handler: Async<Result<int * int * byte[]>> -> Async<unit>) =
    let readLoop () = asyncResult {
      let! sizeBytes = client.ReadAsync 4
      let! size = FsKafka.Protocol.Response.decodeInt sizeBytes
      let! correlatorBytes = client.ReadAsync 4
      let! correlator = FsKafka.Protocol.Response.decodeInt correlatorBytes
      let! messageData = client.ReadAsync (size - 4)
      return (size, correlator, messageData) }

    let rec loop (inbox:MailboxProcessor<unit>) = async {
      let! _ = inbox.Receive()
      do! readLoop() |> handler
      return! loop inbox }

    MailboxProcessor<unit>.Start(loop, cancellationToken)

  type T(config:Config, io: unit -> IClient) =
    let verbosef f         = verbosef config.Log "FsKafka.Connection" f
    let verbosee           = verbosee config.Log "FsKafka.Connection"

    (* Requests related *)
    let correlator         = ref 0
    let nextCorrelator ()  =
      if !correlator > Int32.MaxValue - 1000 then Interlocked.Exchange(correlator, 0) |> ignore
      Interlocked.Increment correlator

    // maybe change to list of requests per client and correlator per client
    // because we can't get response from connection A while request was sent to connection B
    let requests    = new ConcurrentDictionary<int, RequestOrResponse * TaskCompletionSource<RequestOrResponse>>()

    let tryRemoveRequest id = async {
      match requests.TryRemove id with
      | true, (request, source) -> return Success(request, source)
      | false, _                -> return sprintf "couldn't remove request %i" id |> exn |> Failure }

    let trySetResult id (source:TaskCompletionSource<_>) result = async {
      match source.TrySetResult result with
      | true  -> return Success()
      | false -> return sprintf "couldn't set the result for %i" id |> exn |> Failure }

    let handleResponse response = asyncResult {
      let! (size, correlator, data) = response
      let! (request, source) = tryRemoveRequest correlator
      let! decode = FsKafka.Protocol.Response.requestToResponseDecode request
      let! response = decode size correlator data
      do! trySetResult correlator source response }

    let processResponse data = async {
        let! result = handleResponse data
        match result with
        | Success _ -> ()
        | Failure err -> verbosee err "failed parsing response message" }
        
    (* Brokers related *)
    let brokers                = Dictionary<Endpoint, Lazy<Broker>>()
    
    let createConnectionWithReader host port =
      verbosef (fun f -> f "Opening connection to host=%s, port=%i" host port)
      let client = new Client(config.Log, io (), host, port)
      verbosef (fun f -> f "Starting single reader for host=%s, port=%i" host port)
      let reader = readAgent(client, client.CancellationToken, processResponse)
      { Client = client; Reader = reader }

    let addNewBrokers newBrokers =
      newBrokers
      |> Set.iter (fun endpoint -> brokers.Add(endpoint, lazy (createConnectionWithReader endpoint.Host endpoint.Port)))

    let removeOldBrokers oldBrokers =
      oldBrokers
      |> Set.iter (fun b ->
        if brokers.[b].IsValueCreated
        then
          brokers.[b].Force().Client.Close() |> Async.RunSynchronously
          brokers.Remove b |> ignore
        else brokers.Remove b |> ignore )
        
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
      
    let send endpoint request = async {
      let correlationId  = nextCorrelator()
      let encodedRequest = request |> FsKafka.Protocol.Optics.withCorrelator correlationId |> FsKafka.Protocol.Request.encode
      let broker         = brokers.[endpoint].Force()
      // save request to the requests dict
      let completionSource = new TaskCompletionSource<RequestOrResponse>()
      match requests.TryAdd(correlationId, (request, completionSource)) with
      | true  ->
          let! writeResult   = broker.Client.WriteAsync encodedRequest
          match writeResult with
          | Success _ ->
              verbosef (fun f -> f "Request sent: CorrelationId=%i, Message=%A" correlationId request)
              broker.Reader.Post()
              let requestCancellationToken = new CancellationTokenSource(config.RequestTimeoutMs)
              requestCancellationToken.Token.Register(fun () ->
                cancelRequest completionSource correlationId endpoint.Host endpoint.Port) |> ignore
              let! result = Async.AwaitTask completionSource.Task
              return Success result
          | Failure e ->
              verbosee e (sprintf "Write failed to host=%s, port=%i" endpoint.Host endpoint.Port)
              return Failure e
      | false ->
          let e = FailedAddingRequestException()
          verbosee e (sprintf "Write failed to host=%s, port=%i" endpoint.Host endpoint.Port)
          return e |> Failure }

    let trySend request broker = async {
      let! result = wrapUncancellableOperation (send broker request) config.RequestTimeoutMs
      match result with
      | Some(Success r) -> return Some r
      | _               -> return None } |> Async.RunSynchronously
         
    let sendToFirstSuccessfullBroker request = brokers.Keys |> Seq.tryPick (trySend request)

    do
      config.MetadataBrokersList |> List.map (fun (host, port) -> { Host = host; Port = port }) |> Set.ofList |> addNewBrokers

    member x.UpdateBrokers brokers           = updateBrokers brokers
    member x.Send          (broker, request) = send broker request
    member x.TryPick       request           = sendToFirstSuccessfullBroker request

  let create config =
    match config.MetadataBrokersList with
    | [] -> failwith "empty metadata brokers list"
    | _  -> T(config, fun _ -> (tcpClient config.Log) :> IClient)
