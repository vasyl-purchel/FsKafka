namespace FsKafka

module Consumer =
  type Config =
    { Topics : string list }
  let defaultConfig =
    { Topics = [] }

  type T<'a>(config:Config, connection:Connection.T, metadataProvider:MetadataProvider.T, consume:'a -> unit) =
    class end

  let consume<'a> config connection metadataProvider consumeF =
    let consumer = T<'a>(config, connection, metadataProvider, consumeF)
    consumer
