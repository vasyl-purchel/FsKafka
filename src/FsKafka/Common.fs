namespace FsKafka

open System
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
  
module Common =

  type Result<'T> = Success of 'T | Failure of Exception
  type AsyncResult<'T> = Async<Result<'T>>

  module Result =
    let bind f = function
      | Success v -> f v
      | Failure e -> Failure e
    let map f = function
      | Success v -> f v |> Success
      | Failure e -> Failure e
    let get = function
      | Success v -> v
      | Failure e -> sprintf "Expected Result.Success(x) but received Result.Error(%A)" e |> failwith

    type ResultBuilder () =
      member x.Return  v      = Success v
      member x.Bind    (v, f) = bind f v

    let result = ResultBuilder()

    let toAsync v = async { return v }

  let asyncToResult (computation:Async<'T>) = async {
    let! result = Async.Catch computation
    match result with
    | Choice1Of2 r -> return Success r
    | Choice2Of2 e -> return Failure e }
  
  let inline keyValueToTuple (pair:KeyValuePair<_,_>) = pair.Key, pair.Value
    
  let ensureAsyncSingleThread () =
    let correlator = ref 0
    fun computation -> async {
      if Interlocked.Increment correlator = 1
      then
        do! computation
        Interlocked.Decrement correlator |> ignore
      else Interlocked.Decrement correlator |> ignore }
    
  let wrapUncancellableOperation (operation:Async<'a>) timeout =
    let tcs = new System.Threading.Tasks.TaskCompletionSource<'a option>()
    [ async {
        let! result = operation
        tcs.TrySetResult(Some result) |> ignore }
      async { 
        do! Async.Sleep timeout
        tcs.TrySetResult(None) |> ignore } ]
    |> Async.Parallel |> Async.Ignore |> Async.Start
    tcs.Task |> Async.AwaitTask
    
  module AsyncResult =
    let mreturn v = async { return Success v }
    let bind f v = async {
      let! result = v
      match result with
      | Success v -> return! f v
      | Failure e -> return Failure e }
  
    type AsyncResultBuilder () =
      member x.Return  v      = mreturn v
      member x.Bind    (v, f) = bind f v

  let asyncResult = AsyncResult.AsyncResultBuilder()

  type BatchAgent<'T> (batchSize, timeout) =
    let batchEvent = new Event<'T list>()
  
    let triggerEvent messages = batchEvent.Trigger(messages |> List.rev)
  
    let receiveTimedMessage timeout (inbox:MailboxProcessor<_>) = async {
      let  start   = DateTime.Now
      let! msg     = inbox.TryReceive(max 0 timeout)
      let  elapsed = int (DateTime.Now - start).TotalMilliseconds
      return elapsed, msg }
  
    let rec processNextMessage timeLeft messages inbox = async {
      let! (elapsed, msg) = receiveTimedMessage timeLeft inbox
      match msg with
      | Some(m) when List.length messages = batchSize - 1 ->
          triggerEvent (m :: messages)
          return! processNextMessage timeout [] inbox
      | Some(m) ->
          return! processNextMessage (timeLeft - elapsed) (m :: messages) inbox
      | None when List.isEmpty messages ->
          return! processNextMessage timeout [] inbox
      | None ->
          triggerEvent messages
          return! processNextMessage timeout [] inbox }
  
    let agent = MailboxProcessor<'T>.Start(processNextMessage timeout [])

    member x.BatchProduced = batchEvent.Publish
    member x.Enqueue v     = agent.Post v
  
  let batchAgent<'T> (batchSize, timeout) = BatchAgent<'T>(batchSize, timeout)

  let readAgent (readAsync:         int * CancellationToken -> AsyncResult<byte[]>)
                (codec:             byte[] -> AsyncResult<int>)
                (cancellationToken: CancellationToken)
                (handler:           AsyncResult<int * int * byte[]> -> Async<unit>) =
    let readLoop () = asyncResult {
      let! sizeBytes       = readAsync(4, cancellationToken)
      let! size            = codec sizeBytes
      let! correlatorBytes = readAsync(4, cancellationToken)
      let! correlator      = codec correlatorBytes
      let! messageData     = readAsync (size - 4, cancellationToken)
      return (size, correlator, messageData) }

    let rec loop (inbox:MailboxProcessor<unit>) = async {
      let! _ = inbox.Receive()
      do! readLoop() |> handler
      return! loop inbox }

    MailboxProcessor<unit>.Start(loop, cancellationToken)

  exception PassageDeclinedException of unit
  
  type AsyncCheckpoint() =
    [<VolatileField>]
    let mutable m_tcs = ref (new TaskCompletionSource<bool>());
    
    let rec reset () =
      let tcs = !m_tcs
      if tcs.Task.IsCompleted && (Interlocked.CompareExchange(m_tcs, new TaskCompletionSource<bool>(), tcs) <> tcs)
      then reset()

    let set result =
      let tcs = !m_tcs
      Task.Factory.StartNew(
        System.Func<obj,_>(fun s -> (s :?> TaskCompletionSource<bool>).TrySetResult(result)),
        state             = tcs,
        cancellationToken = CancellationToken.None,
        creationOptions   = TaskCreationOptions.PreferFairness,
        scheduler         = TaskScheduler.Default) |> ignore
      tcs.Task.Wait()

    member x.OnPassage       computation = async {
      let! passage = Async.AwaitTask (!m_tcs).Task
      match passage with
      | true  -> return! computation
      | false -> return PassageDeclinedException() |> Failure }

    member x.WithClosedDoors computation = async {
      reset()
      do! computation
      set true }

    member x.Cancel          ()          = set false

module Crc32 =
  let defaultPolynomial = 0xedb88320u
  let defaultSeed       = 0xFFffFFffu
  let table             =
    let inline nextValue acc =
      if 0u <> (acc &&& 1u) then defaultPolynomial ^^^ (acc >>> 1) else acc >>> 1
    let rec iter k acc =
      if k = 0 then acc else iter (k-1) (nextValue acc)
    [| 0u .. 255u |] |> Array.map (iter 8)
  
  let calculate =
    let inline f acc (x:byte) =
      table.[int32 ((acc ^^^ (uint32 x)) &&& 0xffu)] ^^^ (acc >>> 8)
    Array.fold f defaultSeed >> (^^^) defaultSeed

module Compression =

  open System.IO  
  open System.IO.Compression
  open Snappy

  let private apply compressionType compressionMode (data:byte[]) =
    use source = new MemoryStream(data)
    use destination = new MemoryStream()
    use compresser = compressionType destination compressionMode false
    source.CopyTo(compresser)
    destination.ToArray()

  let private compress   f data = apply f CompressionMode.Compress   data
  let private decompress f data = apply f CompressionMode.Decompress data

  let gzipCompress =
    compress   (fun (x:MemoryStream) (y:CompressionMode) (z:bool) -> new GZipStream(stream = x, mode = y, leaveOpen = z) :> Stream)
  
  let gzipDecompress =
    decompress (fun (x:MemoryStream) (y:CompressionMode) (z:bool) -> new GZipStream(stream = x, mode = y, leaveOpen = z) :> Stream)

  let snappyCompress =
    compress   (fun x y z -> new SnappyStream(stream = x, mode = y, leaveOpen = z) :> Stream)
  
  let snappyDecompress =
    decompress (fun x y z -> new SnappyStream(stream = x, mode = y, leaveOpen = z) :> Stream)