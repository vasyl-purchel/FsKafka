#r "bin/Debug/FsKafka.dll"

open FsKafka

let endpoints = [ "192.168.99.100", 9092 ]
let connection = Connection.create { Connection.defaultConfig with MetadataBrokersList = endpoints }
let metadataProvider = MetadataProvider.create MetadataProvider.defaultConfig connection
let messageCodec (s:string) = System.Text.Encoding.UTF8.GetBytes(s)
let producer = Producer.start<string> {Producer.defaultConfig messageCodec with ProducerType = Producer.Async} connection metadataProvider
Producer.send producer "hello" [||] "Test message"

let rec loop () =
  let line = System.Console.ReadLine()
  match line with
  | "quit" -> ()
  | msg    ->
      Producer.send producer "test" [||] "Test message"
      printfn ">> Sent %s" msg
      loop ()
loop ()

//let consume message = printfn "Received message: %A" message
//Consumer.consume { Consumer.defaultConfig with Topics = ["test"] } connection metadataProvider consume

type BasicStroke = BasicStroke of float
type Color = Red | Black
type Paint = Color of Color

type ShapeStyle = { Stroke:BasicStroke; Paint:Paint }
let defaultStyle = { Stroke = BasicStroke(3.0); Paint = Color Black }
// create ShapeStyle from magic int number like in program
let style = function
  | 1 -> { defaultStyle with Stroke = BasicStroke(3.0) }
  // more styles here (can use not integers but enum)
  | _ -> failwith "unsupported style"
  
type ShapeData  = { Width:int; Height:int }
type Shapes =
  | Rectangle of ShapeData
  | Hexagon
  //...

let getShape style = function
  | Rectangle -> 