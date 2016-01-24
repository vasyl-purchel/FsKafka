# FsKafka

F# client for [apache kafka][1]

Places I'm looking to for ideas and to find how it should be done:

 * [kafka-net][2] - native C# client for Kafka queue servers
 * [FsPickler][3] - serialization library built with [pickler combinators][4]

## Building

Using FAKE tasks from build.fsx, default one will build and run tests
Or just run build.cmd or build.sh

## How I want it to be used

```fsharp
let options = { Kafka.defaults with Servers=["http://CSDKAFKA01:9092"; "http://CSDKAFKA02:9092"] }
let conn = Kafka.connection options

Kafka.consumeAsync { Kafka.defaultConsumer with Topic = "TestTopic" } (fun m ->
  printfn "Received message: P%s,O%s : %s" m.Meta.PartitionId m.Meta.Offset m.Value)

let rec loop () =
  System.Console.ReadLine()
  |> function
     | "quit" -> printfn "Finishing..."
     | message ->
         client.SendMessagesAsync("TestTopic", [|message|])
         loop()
printfn "Type a message and press enter... (to exit enter 'quit')"
loop()
```

## Decisions

During development of encode/decode process for protocol I was choosing between
functions that would use reflection and recursively encode/decode any request
or response, but benchmarks show that this was a bad idea. For 100000 simple
metadata requests on the laptop encoding with `request.Encode()` took **~0.3sec**
while recursive reflection **~12sec**, even with cache of precomputed record
readers and constructors it improved to **~7-8sec**.
Inspired by [FsPickler][3] and [picklers combinators][4] I decided to create
similar idea and created `FsKafka.Pickle` and `FsKafka.Unpickle` with
`Kafka.Codec` that using them to encode requests and decode responses

## TODO

 * make sure it still builds on linux and add CI config
 * make protocol types `internal`
 * create connection, tcpSocket, add encoders and decoders for protocol entries
 * create consumer and producer

## Review

@cloudroutine, @vaskir

[1]: http://kafka.apache.org/
[2]: https://github.com/Jroland/kafka-net
[3]: http://nessos.github.io/FsPickler/
[4]: http://lambda-the-ultimate.org/node/2243
