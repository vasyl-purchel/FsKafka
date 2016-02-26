namespace FsKafka

open System
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
  
module Common =

  type Result<'T> = Success of 'T | Failure of Exception

  let asyncToResult (computation:Async<'T>) = async {
    let! result = Async.Catch computation
    match result with
    | Choice1Of2 r -> return Success r
    | Choice2Of2 e -> return Failure e }
  
  let maybe f = function
    | Success value -> f value |> Success
    | Failure error -> error   |> Failure
  
  let inline keyValueToTuple (pair:KeyValuePair<_,_>) = pair.Key, pair.Value

  let ensureSingleThread () =
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
    type AsyncResult<'T> = Async<Result<'T>>
  
    let mreturn v = async { return Success v }
    let bind (v, f) = async {
      let! result = v
      match result with
      | Success v -> return! f v
      | Failure e -> return Failure e }
  
    type AsyncResultBuilder () =
      member x.Return  v      = mreturn v
      member x.Bind    (v, f) = bind (v, f)

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

  type AsyncCheckpoint(name:string) =
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
      | false -> return exn "passage declined" |> Failure }

    member x.WithClosedDoors computation = async {
      reset()
      do! computation
      set true }

    member x.Cancel          ()          = set false