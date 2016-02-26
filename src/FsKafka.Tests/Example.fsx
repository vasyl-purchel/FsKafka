#r "bin/Debug/FsKafka.dll"

open FsKafka

let endpoints = [ "192.168.99.100", 9092 ]
let connection = Connection.create { Connection.defaultConfig with MetadataBrokersList = endpoints }
let metadataProvider = MetadataProvider.create MetadataProvider.defaultConfig connection
let messageCodec (s:string) = System.Text.Encoding.UTF8.GetBytes(s)
let producer = Producer.start<string> (Producer.defaultConfig messageCodec) connection metadataProvider

let rec loop () =
  let line = System.Console.ReadLine()
  match line with
  | "quit" -> ()
  | msg    ->
      Producer.send producer "hello" [||] "Test message"
      printfn ">> Sent %s" msg
      loop ()
loop ()

//let consume message = printfn "Received message: %A" message
//Consumer.consume { Consumer.defaultConfig with Topics = ["test"] } connection metadataProvider consume
