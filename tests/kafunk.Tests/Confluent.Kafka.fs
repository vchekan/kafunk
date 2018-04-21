namespace Confluent.Kafka

open System
open System.Text
open System.Threading
open System.Threading.Tasks
open Confluent.Kafka
open System.Collections.Generic

type TopicName = string

type Partition = int

[<AutoOpen>]
module internal Prelude =
  
  let inline isNull (x:obj) = obj.ReferenceEquals (x,null)

  let inline arrayToSegment (xs:'a[]) : ArraySegment<'a> =
    ArraySegment (xs, 0, xs.Length)

  let inline stringToSegment (s:string) : ArraySegment<byte> =
    if isNull s then ArraySegment()
    else Encoding.UTF8.GetBytes s |> arrayToSegment

[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module Message =
  
  let internal errorMessage (m:Message) (e:Error) =
    sprintf "message_error|topic=%s partition=%i offset=%i code=%O reason=%s" m.Topic m.Partition m.Offset.Value e.Code e.Reason

  let internal errorException (m:Message) (e:Error) =
    exn(errorMessage m e)

  let ensureNotError (m:Message) =
    if m.Error.HasError then
      raise (errorException m m.Error)

  let keyString (m:Message) =
    Encoding.UTF8.GetString m.Key

  let valueString (m:Message) =
    Encoding.UTF8.GetString m.Value

/// Kafka connection configuration.
type KafkaConfig = {
  
  /// bootstrap.servers
  host : string
  
  /// client.id
  clientId : string
  
  /// max.inflight
  maxInFlight : int
  
  /// socket.timeout.ms
  socketTimeoutMs : int

  /// socket.send.buffer.bytes
  socketSendBufferBytes : int

  /// socket.receive.buffer.byte
  socketReceiveBufferBytes : int

} with
  
  static member Create () = ()

  /// Creates a collection of configuration values to be passed to Confluent.Kafka.
  member __.ToList () : list<string * obj> =
    [
      "bootstrap.servers", box __.host
      "client.id", box __.clientId
      "max.in.flight.requests.per.connection", box __.maxInFlight
    ]

/// Producer configuration.
type ProducerConfig = {
  requiredAcks : RequiredAcks
  compression : CompressionCodec
  batchLingerMs : int
  batchSizeCount : int
} with
  
  static member Create () = ()

  /// Creates a collection of configuration values to be passed to Confluent.Kafka.
  member __.ToList () : list<string * obj> =
    [
      "acks", box (__.requiredAcks.ToString())
      "batch.num.messages", box __.batchSizeCount
      "linger.ms", box __.batchLingerMs
    ]

/// Producer compression codec.
and CompressionCodec =
  | None
  | GZip
  | Snappy
  | LZ4
  with  
    override __.ToString () =
      match __ with
      | None -> "none"
      | GZip -> "gzip"
      | Snappy -> "snappy"
      | LZ4 -> "lz4"

/// Producer required acks.
and RequiredAcks =
  | AllInSync
  | Local
  | None
  with 
    override __.ToString () =
      match __ with
      | AllInSync -> "all"
      | Local -> "local"
      | None -> "none"

/// Operations on producers.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module Producer =
  
  let createConfig (config:seq<string * obj>) =
    let producer = new Producer(dict config, false, false)
    producer    

  let create (ccfg:KafkaConfig) (pcfg:ProducerConfig) =
    let config = Seq.append (ccfg.ToList()) (pcfg.ToList())
    createConfig config

  /// Creates an IDeliveryHandler and a Task which completes when the handler receives the specified number of messages.
  /// @expectedMessageCount - the number of messages to be received to complete the Task.
  /// @throwOnError - if true, then sets the Task to the faulted state upon receipt of a Message with an Error.
  let private batchDeliveryHandler (expectedMessageCount:int) (throwOnError:bool) : IDeliveryHandler * Task<Message[]> =
    let tcs = TaskCompletionSource<Message[]>()
    let rs = Array.zeroCreate expectedMessageCount
    let mutable i = -1
    let handler = 
      { new IDeliveryHandler with
          member __.HandleDeliveryReport m =
            if throwOnError && m.Error.HasError then
              tcs.TrySetException ((Message.errorException m m.Error)) |> ignore
            let i' = Interlocked.Increment &i
            rs.[i'] <- m
            if i' = expectedMessageCount - 1 then
              tcs.SetResult rs
          member __.MarshalData = false }
    handler,tcs.Task

  let private produceInternal (p:Producer) (topic:string) (throwOnError:bool) (k:ArraySegment<byte>, v:ArraySegment<byte>) : Async<Message> = async {
    let! m = p.ProduceAsync (topic, k.Array, k.Offset, k.Count, v.Array, v.Offset, v.Count, true) |> Async.AwaitTask
    if throwOnError then Message.ensureNotError m
    return m }

  let private produceBatchedInternal (p:Producer) (topic:string) (throwOnError:bool) (ms:(ArraySegment<byte> * ArraySegment<byte>)[]) : Async<Message[]> = async {
    let handler,result = batchDeliveryHandler ms.Length throwOnError
    for i = 0 to ms.Length - 1 do
      let k,v = ms.[i]
      p.ProduceAsync(topic, k.Array, k.Offset, k.Count, v.Array, v.Offset, v.Count, true, handler)
    return! result |> Async.AwaitTask }

  let produce (p:Producer) (topic:string) (k:ArraySegment<byte>, v:ArraySegment<byte>) : Async<Message> =
    produceInternal p topic true (k,v)

  let produceBytes (p:Producer) (topic:string) (k:byte[], v:byte[]) : Async<Message> =
    produceInternal p topic true (arrayToSegment k, arrayToSegment v)

  let produceString (p:Producer) (topic:string) (k:string, v:string) : Async<Message> =
    produce p topic (stringToSegment k, stringToSegment v)

  let produceBatched (p:Producer) (topic:string) (ms:(ArraySegment<byte> * ArraySegment<byte>)[]) : Async<Message[]> =
    produceBatchedInternal p topic true ms

  let produceBatchedBytes (p:Producer) (topic:string) (ms:(byte[] * byte[])[]) : Async<Message[]> =
    let ms = ms |> Array.map (fun (k,v) -> arrayToSegment k, arrayToSegment v)
    produceBatched p topic ms

  let produceBatchedString (p:Producer) (topic:string) (ms:(string * string)[]) : Async<Message[]> =
    let ms = ms |> Array.map (fun (k,v) -> stringToSegment k, stringToSegment v)
    produceBatched p topic ms


/// Consumer configuration.
type ConsumerConfig = {
  topic : string
  groupId : string
  fetchMaxBytes : int
  fetchMinBytes : int
  fetchMaxWait : int
  autoOffsetReset : AutoOffsetReset
  heartbeatInternvalMs : int
  sessionTimeoutMs : int
  checkCrc : bool
} with
  /// Creates a collection of configuration values to be passed to Confluent.Kafka.
  member __.ToList () : list<string * obj> =
    [
      "group.id", box __.groupId        
      "default.topic.config", box <| (dict ["auto.offset.reset", box (__.autoOffsetReset.ToString())])
      "enable.auto.commit", box false
      "fetch.message.max.bytes", box __.fetchMaxBytes
      "fetch.min.bytes", box __.fetchMinBytes
      "fetch.wait.max.ms", box __.fetchMaxWait
      "check.crcs", box __.checkCrc
      "heartbeat.interval.ms", box __.heartbeatInternvalMs
      "session.timeout.ms", box __.sessionTimeoutMs
    ]

/// Consumer offset reset.
and AutoOffsetReset =
  | Error
  | Earliest
  | Latest
  with
    override __.ToString () =
      match __ with
      | Error -> "error"
      | Earliest -> "earliest"
      | Latest -> "latest"

/// A set of messages from a single partition.
type ConsumerMessageSet = {
  topic : TopicName
  partition : Partition
  messages : Message[]
} with
  static member lastOffset (ms:ConsumerMessageSet) =
    if ms.messages.Length = 0 then -1L
    else ms.messages.[ms.messages.Length - 1].Offset.Value

  static member firstOffset (ms:ConsumerMessageSet) =
    if ms.messages.Length = 0 then -1L
    else ms.messages.[0].Offset.Value

/// Operations on consumers.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module Consumer =

  let createConfig (config:seq<string * obj>) =
    let consumer = new Consumer(dict config)
    consumer.OnPartitionsAssigned |> Event.add (fun m -> consumer.Assign m)
    consumer

  let create (ccfg:KafkaConfig) (cfg:ConsumerConfig) =
    let config = Seq.append (ccfg.ToList()) (cfg.ToList())
    let consumer = createConfig config
    consumer.Subscribe cfg.topic
    consumer

  let private handlePartitioned (ms:Message[]) (handle:ConsumerMessageSet -> Async<unit>) =
    ms 
    |> Array.groupBy (fun m -> m.Topic, m.Partition) 
    |> Seq.map (fun ((t,p),ms) -> handle { ConsumerMessageSet.topic = t ; partition = p ; messages = ms })
    |> Async.Parallel
    |> Async.Ignore

  let consume (c:Consumer) (timeoutMs:int) (batchSize:int) (handle:ConsumerMessageSet -> Async<unit>) : Async<unit> = async {
    let! ct = Async.CancellationToken
    let mutable m = Unchecked.defaultof<_>
    let mutable last = DateTime.UtcNow
    let buf = ResizeArray<_>()
    while not ct.IsCancellationRequested do
      let now = DateTime.UtcNow
      let dt = int (DateTime.UtcNow - last).TotalMilliseconds
      last <- now
      let timeoutMs = max (timeoutMs - dt) 0
      if timeoutMs > 0 && c.Consume(&m, timeoutMs) then
        Message.ensureNotError m
        buf.Add m
        if buf.Count >= batchSize then
          let arr = buf.ToArray()
          do! handlePartitioned arr handle
          buf.Clear ()
      else
        if buf.Count > 0 then
          let arr = buf.ToArray()
          do! handlePartitioned arr handle
          buf.Clear () }

  // -------------------------------------------------------------------------------------------------
  // AsyncSeq adapters

  open FSharp.Control

  /// Represents repeated calls to Consumer.Consume with the specified timeout as an AsyncSeq.
  /// If a message is marked as an error, raises an exception.
  let stream (c:Consumer) (timeoutMs:int) : AsyncSeq<Message> = asyncSeq {
    let mutable m = Unchecked.defaultof<_>
    while true do
      if c.Consume(&m, timeoutMs) then
        Message.ensureNotError m
        yield m }

  /// Represents repeated calls to Consumer.Consume with the specified timeout as an AsyncSeq.
  /// Buffers messages into buffers bounded by time and count.
  let streamBuffered (c:Consumer) (timeoutMs:int) (batchSize:int) : AsyncSeq<Message[]> = asyncSeq {
    let mutable m = Unchecked.defaultof<_>
    let mutable last = DateTime.UtcNow
    let buf = ResizeArray<_>()
    while true do
      let now = DateTime.UtcNow
      let dt = int (DateTime.UtcNow - last).TotalMilliseconds
      last <- now
      let timeoutMs = max (timeoutMs - dt) 0
      if timeoutMs > 0 && c.Consume(&m, timeoutMs) then
        Message.ensureNotError m
        buf.Add m
        if buf.Count >= batchSize then
          yield buf.ToArray()
          buf.Clear ()
      else
        if buf.Count > 0 then
          yield buf.ToArray()
          buf.Clear () }

  /// Represents repeated calls to Consumer.Consume with the specified timeout as an AsyncSeq.
  /// Buffers messages into buffers bounded by time and count.
  /// Partitions buffers by message partition.
  let streamBufferedPartitioned (c:Consumer) (timeoutMs:int) (batchSize:int) : AsyncSeq<(int * Message[])[]> =
    streamBuffered c timeoutMs batchSize 
    |> AsyncSeq.map (Array.groupBy (fun m -> m.Partition))
