namespace FsKafka

open System
open System.Threading

module Logging =
  type LogLevel =
    | Verbose
    | Debug
    | Info
    | Warn
    | Error
    | Fatal

  and  LogLine  =
    { Message:   string
      Level:     LogLevel
      Path:      string
      Exception: exn option
      Worker:    int
      Timestamp: int64 }

  and  Logger   =
    { LogLine : LogLevel -> (unit -> LogLine) -> unit }
  
  let logLevelToInt    = function
    | Verbose -> 1
    | Debug   -> 2
    | Info    -> 3
    | Warn    -> 4
    | Error   -> 5
    | Fatal   -> 6
    
  let logLevelToString = function
    | Verbose -> "VERBOSE"
    | Debug   -> "DEBUG"
    | Info    -> "INFO"
    | Warn    -> "WARN"
    | Error   -> "ERROR"
    | Fatal   -> "FATAL"

  let mkLogLine path level ex message =
    fun _ ->
      { Message   = message
        Level     = level
        Path      = path
        Exception = ex
        Worker    = Thread.CurrentThread.ManagedThreadId
        Timestamp = DateTime.UtcNow.Ticks }

  let verbose  (logger : Logger) path message =
    logger.LogLine LogLevel.Verbose (mkLogLine path LogLevel.Verbose None message)
    
  let verbosef (logger : Logger) path fFormat =
    fFormat (Printf.kprintf (verbose logger path))

  let verbosee (logger : Logger) path ex message =
    logger.LogLine LogLevel.Verbose (mkLogLine path LogLevel.Verbose (Some ex) message)

  let info     (logger : Logger) path message =
    logger.LogLine LogLevel.Info (mkLogLine path LogLevel.Info None message)

  let infof    (logger : Logger) path fFormat =
    fFormat (Printf.kprintf (info logger path))

  let infoe    (logger : Logger) path ex message =
    logger.LogLine LogLevel.Info (mkLogLine path LogLevel.Info (Some ex) message)

  let log      (logger : Logger) path level message =
    logger.LogLine level (mkLogLine path level None message)

  let valid    minLevel level =
    (minLevel |> logLevelToInt) <= (level |> logLevelToInt)

  let defaultLogger minLevel =
    { LogLine = fun level mkLine ->
        if valid minLevel level then
          // 2014-04-05T12:34:56Z [ 13] DEBUG: my.sample.app - Hello World!
          let line      = mkLine()
          let timestamp = (DateTime(line.Timestamp, DateTimeKind.Utc).ToString("o"))
          let formatted = sprintf "%s [%3i] %-5s: %s - %s" timestamp line.Worker (level |> logLevelToString) line.Path line.Message
          match line.Exception with
          | Some ex -> printfn "%s\nexn:%s" formatted (ex.ToString())
          | None    -> printfn "%s" formatted }
  
  let combine loggers =
    { LogLine = fun level mkLine ->
        loggers |> List.iter(fun logger -> logger.LogLine level mkLine) }
