namespace FsKafka

open System
open System.Text
open System.Linq
open System.IO

module Pickle =

  type PStream = byte[] list
  type Pickler<'a> = 'a -> PStream -> PStream

  let private forceBigEndian (value:byte[]) =
    if BitConverter.IsLittleEndian then value |> Array.rev else value
    
  let private copyBlock toBuffer block toOffset =
    Buffer.BlockCopy(block, 0, toBuffer, toOffset, block.Length)
    toOffset + block.Length

  let pZero () : byte[] list = []
  let pUnit element stream   = element :: stream
  let pInt8 (v:SByte)        = pUnit ([|byte v|] |> forceBigEndian)
  let pInt16 (v:int16)       = pUnit (BitConverter.GetBytes(v) |> forceBigEndian)
  let pInt32 (v:int32)       = pUnit (BitConverter.GetBytes(v) |> forceBigEndian)
  let pInt64 (v:int64)       = pUnit (BitConverter.GetBytes(v) |> forceBigEndian)
  let pBytes (v:byte[])      = (pInt32 (if v = null then -1 else v.Length)) >> (pUnit v)
  let pString (v:string)     =
    let size = if String.IsNullOrEmpty v then -1s else Operators.int16 v.Length
    (pInt16 size) >> (pUnit (Encoding.UTF8.GetBytes(v)))
    
  let pList pickler (v:'a list) =
    let f v s = List.fold(fun s e -> pickler e s) s v
    (pInt32 (v.Length)) >> (f v)

  let encode (pickler:Pickler<'a>) value =
    let stream = pZero() |> pickler value
    let buffer = Array.zeroCreate<byte> (stream |> List.sumBy (fun e -> e.Length))
    List.foldBack (copyBlock buffer) stream 0 |> ignore
    buffer

module Unpickle =

  type OutOfBoundariesError =
    { Size:       int
      Offset:     int
      StreamSize: int }
  type UnfinishedParsingError =
    { Offset:     int
      StreamSize: int }
  type UpErrors =
    | OutOfBoundaries of OutOfBoundariesError
    | UnfinishedParsing of UnfinishedParsingError

  type UpStream = byte[] * int

  type UpResult<'a> =
    | Success of 'a * UpStream
    | Failure of UpErrors
  
  type Unpickler<'a> = UpStream -> UpResult<'a>

  type UnpickleBuilder() =
    member x.Bind (v, f)  =
      match v with
      | Success(r, s) -> f (r, s)
      | Failure err   -> Failure err
    member x.ReturnFrom m = m
    
  let unpickle = UnpickleBuilder()

  let upPair      uA uB          stream = unpickle { let! (a, streamA) = uA stream
                                                     let! (b, streamB) = uB streamA
                                                     return! Success((a, b), streamB) }
  let upTriple    uA uB uC       stream = unpickle { let! ((a, b), streamB) = upPair uA uB stream
                                                     let! (c, streamC) = uC streamB
                                                     return! Success((a, b, c), streamC) }
  let upQuadruple uA uB uC uD    stream = unpickle { let! ((a, b, c), streamC) = upTriple uA uB uC stream
                                                     let! (d, streamD) = uD streamC
                                                     return! Success((a, b, c, d), streamD) }
  let upQuintuple uA uB uC uD uE stream = unpickle { let! ((a, b, c, d), streamD) = upQuadruple uA uB uC uD stream
                                                     let! (e, streamE) = uE streamD
                                                     return! Success((a, b, c, d, e), streamE) }
    
  let private forceBigEndian (value:byte[]) =
    if BitConverter.IsLittleEndian then value |> Array.rev else value

  let private decodePart size forceBigEndian f (data:byte[], offset:int) =
    if data.Length <= size + offset - 1
    then Failure(UpErrors.OutOfBoundaries{ Size = size; Offset = offset; StreamSize = data.Length })
    else
      let value = Array.init size (fun i -> data.[i + offset])
      if forceBigEndian && BitConverter.IsLittleEndian
      then Success(f (Array.rev value), (data, offset + size))
      else Success(f value,             (data, offset + size))

  let upInt8   = decodePart 1 false (fun v -> Convert.ToSByte v.[0])
  let upInt16  = decodePart 2 true  (fun v -> BitConverter.ToInt16(v, 0))
  let upInt32  = decodePart 4 true  (fun v -> BitConverter.ToInt32(v, 0))
  let upInt64  = decodePart 8 true  (fun v -> BitConverter.ToInt64(v, 0))
  let upBytes  stream =
    match upInt32 stream with
    | Success(-1,   stream) -> Success(null, stream)
    | Success(size, stream) -> decodePart size false id stream
    | Failure err           -> Failure err
  let upString stream =
    match upInt16 stream with
    | Success(-1s,  stream) -> Success(null, stream)
    | Success(size, stream) -> decodePart (int size) false Encoding.UTF8.GetString stream
    | Failure err           -> Failure err
    
  let upList<'a> (unpickler:Unpickler<'a>) stream =
    let rec f count g (state:UpResult<'a list>) =
      match count with
      | 0 -> match state with
             | Success(l, s) -> Success(l |> List.rev, s)
             | Failure err   -> Failure err
      | c -> f (count - 1) g (g state)
    let g u state =
      match state with
      | Success(list, stream) ->
          match u stream with
          | Success(v, stream) -> Success(v::list, stream)
          | Failure err        -> Failure err
      | Failure err  -> Failure err
    
    match upInt32 stream with
    | Success(size, newStream) -> f size (g unpickler) (Success([], newStream))
    | Failure err              -> Failure err
    
  let decode (u:Unpickler<'a>) data = unpickle {
    let! (a, (data, offset)) = u (data, 0)
    return! Success(a, (data, offset)) }
    
module Crc32 =
  let defaultPolynomial = 0xedb88320u
  let defaultSeed       = 0xFFffFFffu
  let table             =
    let inline nextValue acc =
      if 0u <> (acc &&& 1u) then defaultPolynomial ^^^ (acc >>> 1) else acc >>> 1
    let rec iter k acc =
      if k = 0 then acc else iter (k-1) (nextValue acc)
    [| 0u .. 255u |] |> Array.map (iter 8)
  
  let calculate =
    let inline f acc (x:byte) =
      table.[int32 ((acc ^^^ (uint32 x)) &&& 0xffu)] ^^^ (acc >>> 8)
    Array.fold f defaultSeed >> (^^^) defaultSeed

module Compression =
  
  open System.IO.Compression
  open Snappy

  let private apply compressionType compressionMode (data:byte[]) =
    use source = new MemoryStream(data)
    use destination = new MemoryStream()
    use compresser = compressionType destination compressionMode false
    source.CopyTo(compresser)
    destination.ToArray()

  let private compress   f data = apply f CompressionMode.Compress   data
  let private decompress f data = apply f CompressionMode.Decompress data

  let gzipCompress =
    compress   (fun x y z -> new GZipStream(stream = x, mode = y, leaveOpen = z) :> Stream)
  
  let gzipDecompress =
    decompress (fun x y z -> new GZipStream(stream = x, mode = y, leaveOpen = z) :> Stream)

  let snappyCompress =
    compress   (fun x y z -> new SnappyStream(stream = x, mode = y, leaveOpen = z) :> Stream)
  
  let snappyDecompress =
    decompress (fun x y z -> new SnappyStream(stream = x, mode = y, leaveOpen = z) :> Stream)