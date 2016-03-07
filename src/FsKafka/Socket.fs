module FsKafka.Socket

open FsKafka.Common
open FsKafka.Logging
open System
open System.Threading
open System.Net
open System.Net.Sockets

exception ConnectionFailedException   of string
exception SocketDisconnectedException of unit
exception ConnectionCanceledException of unit

type IAsyncSocket =
  abstract ConnectAsync: string * int * CancellationToken -> AsyncResult<unit>
  abstract WriteAsync  : byte[]       * CancellationToken -> AsyncResult<unit>
  abstract ReadAsync   : int          * CancellationToken -> AsyncResult<byte[]>
  abstract Close       : unit                             -> unit
  
let rec connectAsync (socket:IAsyncSocket)
                     (onSuccess, onError)
                     (host, port, token:CancellationToken)
                     backoffDelay attempts = async {
  if attempts = 0 then
    return ConnectionFailedException "Reached maximum connection retry attempts" |> Failure
  elif token.IsCancellationRequested then
    return ConnectionCanceledException() |> Failure
  else
    let! result = socket.ConnectAsync(host, port, token)
    match result with
    | Success _ ->
        onSuccess()
        return Success()
    | Failure e ->
        onError e
        do! Async.Sleep backoffDelay
        return! connectAsync socket (onSuccess, onError) (host, port, token) (backoffDelay * 2) (attempts - 1) }
      
type ISyncSocket =
  abstract Connect: string * int -> Result<unit>
  abstract Write  : byte[]       -> Result<unit>
  abstract Read   : int          -> Result<byte[]>
  abstract Close  : unit         -> unit

let rec connectSync (socket:ISyncSocket) (onSuccess, onError) (host, port, token:CancellationToken) (backoffDelay:int) attempts =
  if attempts = 0 then
    ConnectionFailedException "Reached maximum connection retry attempts" |> Failure
  elif token.IsCancellationRequested then
    ConnectionCanceledException() |> Failure
  else
    let result = socket.Connect(host, port)
    match result with
    | Success _ ->
        onSuccess()
        Success ()
    | Failure e ->
        onError e
        Thread.Sleep backoffDelay
        connectSync socket (onSuccess, onError) (host, port, token) (backoffDelay * 2) (attempts - 1)

module Tcp =
  let getIpFromDns host = query {
    for ip in Dns.GetHostAddresses host do
    where (ip.AddressFamily = AddressFamily.InterNetwork
           || ip.AddressFamily = AddressFamily.InterNetworkV6)
    headOrDefault }

  let getIp host =
    match IPAddress.TryParse host with
    | true, ip -> ip
    | false, _ -> getIpFromDns host

  let connectAsync (client:TcpClient) host port token = async {
    try return client.ConnectAsync(getIp host, port).Wait(cancellationToken = token) |> Success
    with e -> return Failure e }
    
  let connect (client:TcpClient) host port =
    try client.Connect(getIp host, port) |> Success
    with e -> Failure e

  let readAsync (stream:NetworkStream) size token =
    let rec loop buffer offset = async {
      let read = stream.ReadAsync(buffer, offset, size - offset)
      read.Wait(cancellationToken = token)
      match read.Result, size - offset with
      | 0, _             -> return Failure <| SocketDisconnectedException()
      | r, e when r >= e -> return Success buffer
      | r, _             -> return! loop buffer (offset + r) }

    loop (Array.zeroCreate size) 0
    
  let read (stream:NetworkStream) size =
    let rec loop buffer offset =
      let readResult = stream.Read(buffer, offset, size - offset)
      match readResult, size - offset with
      | 0, _             -> Failure <| SocketDisconnectedException()
      | r, e when r >= e -> Success buffer
      | r, _             -> loop buffer (offset + r)

    loop (Array.zeroCreate size) 0

  let writeAsync (stream:NetworkStream) data token = async {
    try return stream.WriteAsync(data, 0, data.Length).Wait(cancellationToken = token) |> Success
    with e -> return Failure e }

  let write (stream:NetworkStream) data =
    try
      stream.Write(data, 0, data.Length)
      stream.Flush() |> Success
    with e -> Failure e

  let createAsync (logger:Logger) =
    let log f = verbosef logger "FsKafka.Socket.TcpSocket" f
    
    let disposed = ref 0
    let client   = new TcpClient()
    
    let clean () =
      if Interlocked.Increment disposed = 1
      then (client :> IDisposable).Dispose()
    
    { new IAsyncSocket with
        member x.ConnectAsync(host, port, token) = async {
          log (fun f -> f "[%s:%i] Connecting..." host port)
          return! connectAsync client host port token }
        member x.WriteAsync (data,       token) = async {
          log (fun f -> f "[%A] Writing %i bytes" client.Client.RemoteEndPoint data.Length)
          return! writeAsync (client.GetStream()) data token }
        member x.ReadAsync  (size,       token) = async {
          log (fun f -> f "[%A] Reading %i bytes" client.Client.RemoteEndPoint size)
          return! readAsync  (client.GetStream()) size token }
        member x.Close      ()                  = clean ()
      interface IDisposable with
        member x.Dispose() = clean () }
    
  let createSync (logger:Logger) =
    let log f = verbosef logger "FsKafka.Socket.TcpSocket" f
    
    let disposed = ref 0
    let client   = new TcpClient()
    
    let clean () =
      if Interlocked.Increment disposed = 1
      then (client :> IDisposable).Dispose()
    
    { new ISyncSocket with
        member x.Connect (host, port) =
          log (fun f -> f "[%s:%i] Connecting..." host port)
          connect client host port
        member x.Write   data         =
          log (fun f -> f "[%A] Writing %i bytes" client.Client.RemoteEndPoint data.Length)
          write (client.GetStream()) data
        member x.Read    size         =
          log (fun f -> f "[%A] Reading %i bytes" client.Client.RemoteEndPoint size)
          read  (client.GetStream()) size
        member x.Close   ()           = clean ()
      interface IDisposable with
        member x.Dispose() = clean () }
