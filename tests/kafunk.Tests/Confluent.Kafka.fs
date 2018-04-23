namespace Confluent.Kafka

open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Text
open System.Threading
open System.Threading.Tasks
open Confluent.Kafka
open FSharp.Control

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

  let throwIfError (m:Message) =
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
            else
              let i' = Interlocked.Increment &i
              rs.[i'] <- m
              if i' = expectedMessageCount - 1 then
                tcs.SetResult rs
          member __.MarshalData = false }
    handler,tcs.Task

  let private produceInternal (p:Producer) (topic:string) (throwOnError:bool) (k:ArraySegment<byte>, v:ArraySegment<byte>) : Async<Message> = async {
    let! m = p.ProduceAsync (topic, k.Array, k.Offset, k.Count, v.Array, v.Offset, v.Count, true) |> Async.AwaitTask
    if throwOnError then Message.throwIfError m
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


type OffsetCommitQueue = private {
  consumer : Consumer
  mbOffsets : MailboxProcessor<TopicPartitionOffset[]>
  mbFlushes : MailboxProcessor<TaskCompletionSource<unit>>
}

[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module OffsetCommitQueue =
  
  let private mb () = MailboxProcessor.Start (fun _ -> async.Return())

  let private mbStream (mb:MailboxProcessor<_>) = AsyncSeq.replicateInfiniteAsync (async { return! mb.Receive() })

  let private mergeChoice3 (s1:AsyncSeq<'a>) (s2:AsyncSeq<'b>) (s3:AsyncSeq<'c>) =
    AsyncSeq.mergeAll [ s1 |> AsyncSeq.map Choice1Of3 ; s2 |> AsyncSeq.map Choice2Of3 ; s3 |> AsyncSeq.map Choice3Of3 ]

  let create (c:Consumer) =
    { consumer = c ; mbOffsets = mb () ; mbFlushes = mb () }

  let start (q:OffsetCommitQueue) =
    let ticks = AsyncSeq.intervalMs 10000
    let flushes = mbStream q.mbFlushes
    let offsets = mbStream q.mbOffsets
    let commit (offsets:TopicPartitionOffset[]) = async {
      if offsets.Length > 0 then
        let! _ = q.consumer.CommitAsync offsets |> Async.AwaitTask
        ()
      return () }
      //mbStream q.mbOffsets
      //|> AsyncSeq.bufferByTime 1000
      //|> AsyncSeq.map (fun os ->
      //  os
      //  |> Seq.concat
      //  |> Seq.groupBy (fun o -> o.Topic, o.Partition)
      //  |> Seq.map (fun ((t,p),xs) -> 
      //    let o = xs |> Seq.map (fun o -> o.Offset.Value) |> Seq.max
      //    TopicPartitionOffset(t,p,Offset(o)))
      //  |> Seq.toArray)
    mergeChoice3 ticks flushes offsets
    |> AsyncSeq.foldAsync (fun (offsets:TopicPartitionOffset[]) msg -> async {
      match msg with
      | Choice1Of3 _ -> 
        do! commit offsets
        return offsets
      | Choice2Of3 rep ->
        try 
          do! commit offsets
          rep.SetResult ()
        with ex -> 
          rep.SetException ex
        return offsets
      | Choice3Of3 os' ->         
        return offsets }) [||]
    |> Async.Ignore

  let enqueue (q:OffsetCommitQueue) (os:TopicPartitionOffset[]) =
    q.mbOffsets.Post os

  let enqueueMessages (q:OffsetCommitQueue) (ms:Message[]) =
    enqueue q (ms |> Array.map (fun m -> TopicPartitionOffset(m.Topic, m.Partition, m.Offset)))

  let flush (q:OffsetCommitQueue) : Async<unit> = async {
    let tcs = TaskCompletionSource<unit>()
    q.mbFlushes.Post tcs
    return! tcs.Task |> Async.AwaitTask }


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

  /// Consumes messages, buffers them by time and batchSize and calls the specified handler.
  /// The handler is called sequentially within a partition and in parallel across partitions.
  /// @timeoutMs - the poll timeout; for each call, the timeout is reduced by the delta between the current time and the last handler call.
  /// @batchSize - the per-partition batch size limit. When the limit is reached, the handler is called.
  let consume (c:Consumer) (timeoutMs:int) (batchSize:int) (handle:ConsumerMessageSet -> Async<unit>) : Async<unit> = async {
    // TODO: pollTimeout vs batchTimeout
    let! ct = Async.CancellationToken    
    use cts = CancellationTokenSource.CreateLinkedTokenSource ct
    let tcs = TaskCompletionSource<unit>()    
    let bufs = Dictionary<TopicName * Partition, BlockingCollection<Message>>()    
    let close () =
      for kvp in bufs do
        try kvp.Value.CompleteAdding() with _ -> ()
    tcs.Task.ContinueWith(fun (_:Task<unit>) -> cts.Cancel()) |> ignore
    let consumePartition (t:TopicName) (p:Partition) (buf:BlockingCollection<Message>) = async {
      try
        do!
          buf.GetConsumingEnumerable()
          |> AsyncSeq.ofSeq
          |> AsyncSeq.bufferByCountAndTime batchSize timeoutMs
          |> AsyncSeq.iterAsync (fun ms -> handle { ConsumerMessageSet.topic = t ; partition = p ; messages = ms })
      with ex ->
        tcs.TrySetException ex |> ignore }
    try
      let mutable m = Unchecked.defaultof<_>
      while (not ct.IsCancellationRequested) && (not tcs.Task.IsCompleted) do
        if c.Consume(&m, timeoutMs) then
          Message.throwIfError m
          let mutable buf = Unchecked.defaultof<_>
          if not (bufs.TryGetValue((m.Topic,m.Partition),&buf)) then
            buf <- new BlockingCollection<_>(batchSize)
            bufs.Add ((m.Topic,m.Partition),buf)
            Async.Start (consumePartition m.Topic m.Partition buf, cts.Token)
          buf.Add m
      if tcs.Task.IsFaulted then
        raise tcs.Task.Exception
    finally
      close () }

  // -------------------------------------------------------------------------------------------------
  // AsyncSeq adapters

  /// Represents repeated calls to Consumer.Consume with the specified timeout as an AsyncSeq.
  /// If a message is marked as an error, raises an exception.
  let stream (c:Consumer) (timeoutMs:int) : AsyncSeq<Message> = asyncSeq {
    let mutable m = Unchecked.defaultof<_>
    while true do
      if c.Consume(&m, timeoutMs) then
        Message.throwIfError m
        yield m }

  /// Represents repeated calls to Consumer.Consume with the specified timeout as an AsyncSeq.
  /// Buffers messages into buffers bounded by time and count.
  let streamBuffered (c:Consumer) (timeoutMs:int) (batchSize:int) : AsyncSeq<Message[]> = asyncSeq {
    let mutable m = Unchecked.defaultof<_>
    let mutable last = DateTime.UtcNow
    let buf = ResizeArray<_>()
    while true do
      let now = DateTime.UtcNow
      let dt = int (now - last).TotalMilliseconds      
      let timeoutMs = max (timeoutMs - dt) 0
      if timeoutMs > 0 && c.Consume(&m, timeoutMs) then
        last <- now
        Message.throwIfError m
        buf.Add m
        if buf.Count >= batchSize then
          yield buf.ToArray()
          buf.Clear ()
      else
        last <- now
        if buf.Count > 0 then
          yield buf.ToArray()
          buf.Clear () }

  /// Represents repeated calls to Consumer.Consume with the specified timeout as an AsyncSeq.
  /// Buffers messages into buffers bounded by time and count.
  /// Partitions buffers by message partition.
  let streamBufferedPartitioned (c:Consumer) (timeoutMs:int) (batchSize:int) : AsyncSeq<(int * Message[])[]> =
    streamBuffered c timeoutMs batchSize 
    |> AsyncSeq.map (Array.groupBy (fun m -> m.Partition))
