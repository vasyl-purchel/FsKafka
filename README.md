# FsKafka

[![Build Status](https://travis-ci.org/vasyl-purchel/FsKafka.svg?branch=master)](https://travis-ci.org/vasyl-purchel/FsKafka)
[![Build status](https://ci.appveyor.com/api/projects/status/0tvs4krihppac8fj?svg=true)](https://ci.appveyor.com/project/VasylPurchel/fskafka)
[![Gitter](https://badges.gitter.im/vasyl-purchel/FsKafka.svg)](https://gitter.im/vasyl-purchel/FsKafka?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

F# client for [apache kafka][1]

Documentation can be found [here][5]

Places I'm looking to for ideas and to find how it should be done:

 * [kafka-net][2] - native C# client for Kafka queue servers
 * [FsPickler][3] - serialization library built with [pickler combinators][4]

## Building

Using FAKE tasks from build.fsx, default one will build and run tests
Or just run build.cmd or build.sh

## Usage example

```fsharp

#r "FsKafka.dll"
open FsKafka

let brokenEndpoint   = "192.168.99.100", 9089
let validEndpoint    = "192.168.99.100", 9092
let endpoints        = [ brokenEndpoint; validEndpoint ]

let connection       = Connection.create {
    Connection.defaultConfig with
      MetadataBrokersList = endpoints
      ReconnectionAttempts = 3 }

let metadataProvider = MetadataProvider.create MetadataProvider.defaultConfig connection

let messageCodec s   = System.Text.Encoding.UTF8.GetBytes(s = s)
let producerConfig   = { Producer.defaultConfig messageCodec with ProducerType = Producer.Sync }
let syncProducer     = Producer.start<string> producerConfig connection metadataProvider

Producer.send syncProducer "hello" [||] "Test message"

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

## Done

 * General encoding/decoding
 * Logging (in general but not good in components)
 * Protocol
 * TcpSockets
 * Connection
 * MetadataProvider
 * Produce and metadata messages
 * SimpleSyncProducer
 * SimpleAsyncProducer
 * GZip and Snappy producer compression
 
## Performance possible improvements

 * SocketAsyncEventArgs pool
 * Protocol as structs instead of records

## TODO

 * Producer split message if size is too big
 * Higher level producer (with retries on errors from response)
 * write tests
 * make protocol types `internal`
 * throttle number of asynchronous requests to some limit
 * notify if topic is not created and server configured to not create topic... (for now somehow I'm successfully send messages to topics that I didn't create separately...)
 * check how it works on errors like (a. leader change) (b. node down)
 * implement consumer 
 * no need for exceptions everywhere -> Result.Failure to have just message/exception...
 * reading response - we may succeed on reading size + correlationId but fail later, so we already should use this correlatorId for logging...
 * TopicMetadataRefreshIntervalMs implemented in few places and scheduler has cancellationTokenSource, but it is not cleaned at the end...
 * Connection.RequestsPool, Connection.SocketClientsPool, Connection.T, Producer.T - need to implement IDisposable
 
[1]: http://kafka.apache.org/
[2]: https://github.com/Jroland/kafka-net
[3]: http://nessos.github.io/FsPickler/
[4]: http://lambda-the-ultimate.org/node/2243
[5]: http://vasyl-purchel.github.io/FsKafka/
