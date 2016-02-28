# FsKafka

F# client for [apache kafka][1]

Places I'm looking to for ideas and to find how it should be done:

 * [kafka-net][2] - native C# client for Kafka queue servers
 * [FsPickler][3] - serialization library built with [pickler combinators][4]

## Building

Using FAKE tasks from build.fsx, default one will build and run tests
Or just run build.cmd or build.sh

## Usage example

```fsharp

// producer:
let endpoints = [ "192.168.99.100", 9091; "192.168.99.100", 9089]
let connection = Connection.create { Connection.defaultConfig with MetadataBrokersList = endpoints }
let metadataProvider = MetadataProvider.create MetadataProvider.defaultConfig connection

let codec s = System.Text.Encoding.UTF8.GetBytes(s)
let producer = Producer.start<string> (Producer.defaultConfig codec) connection metadataProvider

let rec loop () =
  System.Console.ReadLine()
  |> function
     | "quit" -> printfn "Finishing..."
     | message ->
         Producer.send producer "TestTopic" [||] message
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

While creating **Producer** I have used batching for all messages. Batching per
broker may be better but leader may get changed after we put message into broker
batch and when we will send message we can get some messages lost.

## TODO

 * write tests
 * make sure it still builds on linux and add CI config
 * make protocol types `internal`
 * throttle number of asynchronous requests to some limit
 * metadata refresh throws exception, better to return Result.Failure
 * retry ProduceMessages if they failed
 * impement MessageCodec for topics + single messages (in producer)
 * notify if topic is not created and server configured to not create topic... (for now somehow I'm successfully send messages to topics that I didn't create separately...)
 * implement ProducerType.Sync
 * implement test service that would use producer
 * run test with 1 producer and console consumer to see that it works...
 * check how it works on errors like (a. leader change) (b. node down)
 * implement consumer
 * implement [error codes](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)
 
 ## Bugs
 
 * no need for exceptions everywhere -> Result.Failure to have just message/exception...
 * reading response - we may succeed on reading size + correlationId but fail later, so we already should use this correlatorId for logging...
 * correlatorId for connect process and where it is not available `-1`
 * don't use RequestOrResponse under doing requests and saving the requests
 * don't use RequestOrResponse for responses
 
[1]: http://kafka.apache.org/
[2]: https://github.com/Jroland/kafka-net
[3]: http://nessos.github.io/FsPickler/
[4]: http://lambda-the-ultimate.org/node/2243
