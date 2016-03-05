module FsKafka.Socket

open FsKafka.Common
open FsKafka.Logging
open System
open System.Threading
open System.Net
open System.Net.Sockets

exception ConnectionFailedException   of string
exception SocketDisconnectedException of unit

type IAsyncSocket =
  abstract ConnectAsync: string * int * CancellationToken -> AsyncResult<unit>
  abstract WriteAsync  : byte[]       * CancellationToken -> AsyncResult<unit>
  abstract ReadAsync   : int          * CancellationToken -> AsyncResult<byte[]>
  abstract Close       : unit                             -> unit

type ISyncSocket =
  abstract Connect: string * int -> Result<unit>
  abstract Write  : byte[]       -> Result<unit>
  abstract Read   : int          -> Result<byte[]>
  abstract Close  : unit         -> unit

let rec connectAsync (socket:IAsyncSocket) onSuccess onError connectionDetails backoffDelay = async {
  let! result = socket.ConnectAsync connectionDetails
  match result with
  | Success _ -> onSuccess()
  | Failure e ->
      onError e
      do! Async.Sleep backoffDelay
      return! connectAsync socket onSuccess onError connectionDetails (backoffDelay * 2) }
      
let rec connectSync (socket:ISyncSocket) onSuccess onError connectionDetails (backoffDelay:int) =
  let result = socket.Connect connectionDetails
  match result with
  | Success _ -> onSuccess()
  | Failure e ->
      onError e
      Thread.Sleep backoffDelay
      connectSync socket onSuccess onError connectionDetails (backoffDelay * 2)

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
    try return client.Connect(getIp host, port) |> Success
    with e -> return Failure e }
    
  let connect (client:TcpClient) host port =
    try client.Connect(getIp host, port) |> Success
    with e -> Failure e

  let readAsync (stream:NetworkStream) size token =
    let rec loop buffer offset = async {
      let! readResult = stream.AsyncRead(buffer, offset, size - offset)
      match readResult, size - offset with
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
    try
      let! _ = stream.AsyncWrite(data, 0, data.Length)
      return Success()
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
        member x.Connect(host, port)  =
          log (fun f -> f "[%s:%i] Connecting..." host port)
          connect client host port
        member x.Write   data         =
          log (fun f -> f "[%A] Writing %i bytes" client.Client.RemoteEndPoint data.Length)
          write (client.GetStream()) data
        member x.Read    size         =
          log (fun f -> f "[%A] Reading %i bytes" client.Client.RemoteEndPoint size)
          read  (client.GetStream()) size
        member x.Close      ()                  = clean ()
      interface IDisposable with
        member x.Dispose() = clean () }
