module FsKafka.SocketClient

open FsKafka.Common
open FsKafka.Logging
open FsKafka.Socket
open System
open System.Threading

exception ConnectionNotEstablishedException of unit

type Config =
  { Log                   : Logger
    Codec                 : byte[] -> Result<int>
    ProcessResponseAsync  : AsyncResult<int * int * byte[]> -> Async<unit>
    Host                  : string
    Port                  : int
    ReconnectionAttempts  : int
    ReconnectionBackoffMs : int }
    
type AsyncClient(config: Config, socket: IAsyncSocket) =
  let verbose msg = verbosef config.Log "FsKafka.Connection.Client"   (fun f -> f "[%s:%i]: %s" config.Host config.Port msg)
  let info    msg = infof    config.Log "FsKafka.Connection.Client"   (fun f -> f "[%s:%i]: %s" config.Host config.Port msg)
  let infoe e msg = infoef   config.Log "FsKafka.Connection.Client" e (fun f -> f "[%s:%i]: %s" config.Host config.Port msg)

  let disposed           = ref 0
  let cancellationSource = new CancellationTokenSource()
  let checkpoint         = AsyncCheckpoint()

  let cleanUp () =
    verbose "cleaning resources"
    if Interlocked.Increment disposed = 1 then
      cancellationSource.Cancel()
      checkpoint.Cancel()
      socket.Close()

  let read (size, token) = socket.ReadAsync (size, token)                    |> checkpoint.OnPassage
  let write data         = socket.WriteAsync(data, cancellationSource.Token) |> checkpoint.OnPassage

  let reader = readAgent read (config.Codec >> Result.toAsync) cancellationSource.Token config.ProcessResponseAsync

  let connect =
    let logOk    () = info "connection established"
    let logError e  = infoe e "connection attempt failed, reconnecting..."
    async {
      let! connectionEstablished = connectAsync socket
                                                (logOk, logError)
                                                (config.Host, config.Port, cancellationSource.Token)
                                                config.ReconnectionBackoffMs
                                                config.ReconnectionAttempts
      match connectionEstablished with
      | Success _ -> ()
      | Failure e -> infoe e "Connection not established"; cleanUp() }
  
  do Async.StartImmediate(checkpoint.WithClosedDoors connect, cancellationSource.Token)

  member x.Send  (data, readResponse) = if readResponse then reader.Post()
                                        write data
  member x.Close ()                   = cleanUp ()

  override x.Finalize () = cleanUp()
  interface IDisposable with
    member x.Dispose  () = cleanUp()

type SyncClient(config: Config, socket: ISyncSocket) =
  let verbose msg = verbosef config.Log "FsKafka.Connection.Client"   (fun f -> f "[%s:%i]: %s" config.Host config.Port msg)
  let info    msg = infof    config.Log "FsKafka.Connection.Client"   (fun f -> f "[%s:%i]: %s" config.Host config.Port msg)
  let infoe e msg = infoef   config.Log "FsKafka.Connection.Client" e (fun f -> f "[%s:%i]: %s" config.Host config.Port msg)

  let disposed           = ref 0
  let locker             = obj ()
  let mutable connected  = false
  let cancellationSource = new CancellationTokenSource()

  let cleanUp () =
    verbose "cleaning resources"
    if Interlocked.Increment disposed = 1 then
      cancellationSource.Cancel()
      socket.Close()

  let connect = async {
    let connectionEstablished = lock (locker) (fun _ ->
      let onSuccess() = info "connection established"; connected <- true
      let logError e  = infoe e "connection attempt failed, reconnecting..."
      connectSync socket
                  (onSuccess, logError)
                  (config.Host, config.Port, cancellationSource.Token)
                  config.ReconnectionBackoffMs
                  config.ReconnectionAttempts)
    match connectionEstablished with
    | Success _ -> ()
    | Failure e -> infoe e "Connection not established"; cleanUp() }

  do
    Async.StartImmediate(connect, cancellationSource.Token)

  let request data readResponse =
    if not connected then ConnectionCanceledException() |> Failure
    else Result.result {
      let! _ = socket.Write data
      if not readResponse then return None
      else
        let! sizeBytes       = socket.Read 4
        let! size            = config.Codec sizeBytes
        let! correlatorBytes = socket.Read 4
        let! correlator      = config.Codec correlatorBytes
        let! messageData     = socket.Read (size - 4)
        return Some(size, correlator, messageData) }
  
  member x.Send  (data, readResponse) =
    lock locker (fun _ -> request data readResponse)
  member x.Close ()                   = cleanUp()

  override x.Finalize () = cleanUp()
  interface IDisposable with
    member x.Dispose  () = cleanUp()

type DoubleClient =
  { SyncClient  : Lazy<SyncClient>
    AsyncClient : Lazy<AsyncClient> }