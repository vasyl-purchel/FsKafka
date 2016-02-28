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

  type LogLine  =
    { Message:    string
      Level:      LogLevel
      Path:       string
      Exception:  exn option
      Worker:     int
      Timestamp:  int64 }

  type Logger   =
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
    { Message    = message
      Level      = level
      Path       = path
      Exception  = ex
      Worker     = Thread.CurrentThread.ManagedThreadId
      Timestamp  = DateTime.UtcNow.Ticks }

  let log   logger path level message     =
    logger.LogLine level (fun _ -> mkLogLine path level None message)

  let logf  logger path level fFormat     =
    fFormat (Printf.kprintf (log logger path level))

  let loge  logger path level exn message =
    logger.LogLine level (fun _ -> mkLogLine path level (Some exn) message)

  let logef logger path level exn fFormat =
    fFormat (Printf.kprintf (loge logger path level exn))

  let verbose   logger path message     = log   logger path LogLevel.Verbose message
  let verbosef  logger path fFormat     = logf  logger path LogLevel.Verbose fFormat
  let verbosee  logger path exn message = loge  logger path LogLevel.Verbose exn message
  let verboseef logger path exn fFormat = logef logger path LogLevel.Verbose exn fFormat
    
  let debug   logger path message     = log   logger path LogLevel.Debug message
  let debugf  logger path fFormat     = logf  logger path LogLevel.Debug fFormat
  let debuge  logger path exn message = loge  logger path LogLevel.Debug exn message
  let debugef logger path exn fFormat = logef logger path LogLevel.Debug exn fFormat

  let info   logger path message     = log   logger path LogLevel.Info message
  let infof  logger path fFormat     = logf  logger path LogLevel.Info fFormat
  let infoe  logger path exn message = loge  logger path LogLevel.Info exn message
  let infoef logger path exn fFormat = logef logger path LogLevel.Info exn fFormat
  
  let error   logger path message     = log   logger path LogLevel.Error message
  let errorf  logger path fFormat     = logf  logger path LogLevel.Error fFormat
  let errore  logger path exn message = loge  logger path LogLevel.Error exn message
  let erroref logger path exn fFormat = logef logger path LogLevel.Error exn fFormat
  
  let warn   logger path message     = log   logger path LogLevel.Warn message
  let warnf  logger path fFormat     = logf  logger path LogLevel.Warn fFormat
  let warne  logger path exn message = loge  logger path LogLevel.Warn exn message
  let warnef logger path exn fFormat = logef logger path LogLevel.Warn exn fFormat
  
  let fatal   logger path message     = log   logger path LogLevel.Fatal message
  let fatalf  logger path fFormat     = logf  logger path LogLevel.Fatal fFormat
  let fatale  logger path exn message = loge  logger path LogLevel.Fatal exn message
  let fatalef logger path exn fFormat = logef logger path LogLevel.Fatal exn fFormat

  let valid minLevel level =
    (minLevel |> logLevelToInt) <= (level |> logLevelToInt)

  let defaultLogger minLevel =
    { LogLine = fun level mkLine ->
        if valid minLevel level then
          // 2014-04-05T12:34:56Z [ 13] DEBUG: my.sample.app - [Correlator=13] Hello World!
          let line      = mkLine()
          let formatted =
            sprintf "%s [%3i] %-5s: %s - %s"
              (DateTime(line.Timestamp, DateTimeKind.Utc).ToString("o"))
              line.Worker
              (level |> logLevelToString)
              line.Path
              line.Message
          match line.Exception with
          | Some ex -> printfn "%s\nexn:%s" formatted (ex.ToString())
          | None    -> printfn "%s" formatted }
  
  let combine loggers =
    { LogLine = fun level mkLine ->
        loggers |> List.iter(fun logger -> logger.LogLine level mkLine) }
