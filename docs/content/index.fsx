(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use 
// it to define helpers that you do not want to show in the documentation.
#I "../../bin/FsKafka"

(**
FsKafka
======================

F# native client for [apache kafka](http://kafka.apache.org/)

Installation
-----------------------

The FsKafka library can be installed from [NuGet](https://nuget.org/packages/FsKafka):

    PM> Install-Package FsKafka

Producer example
-------

*)
#r "FsKafka.dll"
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


(**
Some more info

Samples & documentation
-----------------------

 * [API Reference](reference/index.html) contains automatically generated documentation for all types, modules
   and functions in the library. This includes additional brief samples on using most of the
   functions.
 
Contributing and copyright
--------------------------

The project is hosted on [GitHub][gh] where you can [report issues][issues], fork 
the project and submit pull requests. If you're adding a new public API, please also 
consider adding [samples][content] that can be turned into a documentation. You might
also want to read the [library design notes][readme] to understand how it works.

The library is available under Public Domain license, which allows modification and 
redistribution for both commercial and non-commercial purposes. For more information see the 
[License file][license] in the GitHub repository. 

  [content]: https://github.com/fsprojects/FsKafka/tree/master/docs/content
  [gh]: https://github.com/fsprojects/FsKafka
  [issues]: https://github.com/fsprojects/FsKafka/issues
  [readme]: https://github.com/fsprojects/FsKafka/blob/master/README.md
  [license]: https://github.com/fsprojects/FsKafka/blob/master/LICENSE.txt
*)
