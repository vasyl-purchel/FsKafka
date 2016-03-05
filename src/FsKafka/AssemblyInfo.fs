namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("FsKafka")>]
[<assembly: AssemblyProductAttribute("FsKafka")>]
[<assembly: AssemblyDescriptionAttribute("Native F# Apache Kafka client")>]
[<assembly: AssemblyVersionAttribute("0.0.1")>]
[<assembly: AssemblyFileVersionAttribute("0.0.1")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.0.1"
