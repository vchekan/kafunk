﻿namespace Confluent.Kafka

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
  | NoCodec
  | GZip
  | Snappy
  | LZ4
  with  
    override __.ToString () =
      match __ with
      | NoCodec -> "none"
      | GZip -> "gzip"
      | Snappy -> "snappy"
      | LZ4 -> "lz4"

/// Producer required acks.
and RequiredAcks =
  | AllInSyncAck
  | LocalAck
  | NoAck
  with 
    override __.ToString () =
      match __ with
      | AllInSyncAck -> "all"
      | LocalAck -> "local"
      | NoAck -> "none"

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



type private OffsetCommitQueueState = {  
  assignment : Set<string * int>
  offsets : Dictionary<string * int, int64>
} with
  
  static member Empty = { assignment = Set.empty ; offsets = Dictionary<_,_>() }
  
  static member revoke (s:OffsetCommitQueueState) (tps:TopicPartition seq) =
    { assignment = (s.assignment,tps) ||> Seq.fold (fun a tp -> Set.remove (tp.Topic,tp.Partition) a)
      offsets = s.offsets }
      
  static member assign (s:OffsetCommitQueueState) (tps:TopicPartition seq) =
    { assignment = tps |> Seq.map (fun tp -> tp.Topic, tp.Partition) |> set
      offsets = s.offsets }
  
  /// Returns the latest committed offsets respecting the current assignment.
  static member getOffsets (s:OffsetCommitQueueState) =
    s.offsets 
    |> Seq.choose (fun kvp -> 
      let t,p = fst kvp.Key, snd kvp.Key
      if Seq.contains (t,p) s.assignment then TopicPartitionOffset(t, p, Offset(kvp.Value)) |> Some
      else None)
    |> Seq.toArray      
    
  static member updateOffsets (s:OffsetCommitQueueState) (os':TopicPartitionOffset seq) = 
    let mutable o = Unchecked.defaultof<_>
    for o' in os' do
      let tp = o'.Topic,o'.Partition
      if s.offsets.TryGetValue (tp,&o) then s.offsets.[tp] <- max o'.Offset.Value o
      else s.offsets.[tp] <- o'.Offset.Value
    s

/// An accumulating offset commit queue.
type OffsetCommitQueue internal (consumer:Consumer) =   
  
  let mbOffsets : MailboxProcessor<TopicPartitionOffset[]> = MailboxProcessor.Start (fun _ -> async.Return())
  let mbFlushes : MailboxProcessor<TaskCompletionSource<unit>> = MailboxProcessor.Start (fun _ -> async.Return())
  let committed : Event<TopicPartitionOffset[]> = new Event<_>()
  
  let mbStream (mb:MailboxProcessor<'a>) : AsyncSeq<'a> = 
    AsyncSeq.replicateInfiniteAsync (async { return! mb.Receive() })
  
  let mergeChoice5 (s1:AsyncSeq<'a>) (s2:AsyncSeq<'b>) (s3:AsyncSeq<'c>) (s4:AsyncSeq<'d>) (s5:AsyncSeq<'e>) : AsyncSeq<Choice<'a, 'b, 'c, 'd, 'e>> =
    AsyncSeq.mergeAll [ 
      s1 |> AsyncSeq.map Choice1Of5
      s2 |> AsyncSeq.map Choice2Of5
      s3 |> AsyncSeq.map Choice3Of5
      s4 |> AsyncSeq.map Choice4Of5
      s5 |> AsyncSeq.map Choice5Of5 ]
  
  /// Event triggered when enqueued offsets are committed.
  member __.OnOffsetsCommitted = committed.Publish

  /// Starts the offset commit queue process.
  member __.Start (commitIntervalMs:int) = async {
       
    // commits accumulated offsets
    let commit (s:OffsetCommitQueueState) = async {
      let offsets = OffsetCommitQueueState.getOffsets s
      if offsets.Length > 0 then
        //printfn "committing_offsets|%A" offsets
        let! res = consumer.CommitAsync offsets |> Async.AwaitTask
        if (res.Error.HasError) then
          failwithf "offset_commit_error|code=%O reason=%s" res.Error.Code res.Error.Reason
        committed.Trigger offsets
        return () }

    let ticks = AsyncSeq.intervalMs commitIntervalMs
    let flushes = mbStream mbFlushes
    let commits = mbStream mbOffsets
    let revokes = consumer.OnPartitionsRevoked |> AsyncSeq.ofObservableBuffered
    let assigns = consumer.OnPartitionsAssigned |> AsyncSeq.ofObservableBuffered

    return!
      mergeChoice5 ticks flushes commits revokes assigns
      |> AsyncSeq.foldAsync (fun (s:OffsetCommitQueueState) msg -> async {
        match msg with
        | Choice1Of5 _ -> 
          do! commit s
          return s
        | Choice2Of5 rep ->
          try 
            do! commit s
            rep.SetResult ()
          with ex -> 
            rep.SetException ex
          return s
        | Choice3Of5 committedOffsets ->
          return OffsetCommitQueueState.updateOffsets s committedOffsets
        | Choice4Of5 revokedPartitions ->
          return OffsetCommitQueueState.revoke s revokedPartitions 
        | Choice5Of5 assignedPartitions ->
          return OffsetCommitQueueState.assign s assignedPartitions }) OffsetCommitQueueState.Empty
      |> Async.Ignore }

  /// Enqueues offsets to be committed asynchronously.
  member __.Enqueue os = 
    mbOffsets.Post os

  member __.Flush () : Async<unit> = async {
    let tcs = TaskCompletionSource<unit>()
    mbFlushes.Post tcs
    return! tcs.Task |> Async.AwaitTask }

/// Operations of the offset commit queue.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module OffsetCommitQueue =
  
  /// Creates an offset commit queue.
  let create (c:Consumer) = new OffsetCommitQueue(c)

  /// Starts the offset commit queue.
  let start (q:OffsetCommitQueue) (commitIntervalMs:int) =
    q.Start commitIntervalMs

  /// Enqueues offsets to be committed asynchronously.
  let enqueue (q:OffsetCommitQueue) (os:TopicPartitionOffset[]) =
    q.Enqueue os

  /// Flushes all enqueued offsets.
  let flush (q:OffsetCommitQueue) : Async<unit> =
    q.Flush ()

/// Operations on consumers.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module Consumer =

  /// Creates a consumer.
  /// - OnPartitionsAssigned -> Assign
  /// - OnPartitionsRevoked -> Unassign
  let createConfig (config:seq<string * obj>) =
    let consumer = new Consumer(dict config)
    consumer.OnPartitionsAssigned |> Event.add (fun m -> consumer.Assign m)
    consumer.OnPartitionsRevoked |> Event.add (fun _ -> consumer.Unassign ())
    consumer

  /// Creates a consumer.
  let create (ccfg:KafkaConfig) (cfg:ConsumerConfig) =
    let config = Seq.append (ccfg.ToList()) (cfg.ToList())
    let consumer = createConfig config
    consumer.Subscribe cfg.topic
    consumer

  /// Consumes messages, buffers them by time and batchSize and calls the specified handler.
  /// The handler is called sequentially within a partition and in parallel across partitions.
  /// @pollTimeoutMs - the consumer poll timeout.
  /// @batchLingerMs - the time to wait to form a per-partition batch; when time limit is reached the handler is called.
  /// @batchSize - the per-partition batch size limit; when the limit is reached, the handler is called.
  let consume 
    (c:Consumer) 
    (pollTimeoutMs:int)
    (batchLingerMs:int) 
    (batchSize:int) 
    (handle:ConsumerMessageSet -> Async<unit>) : Async<unit> = async {
    let! ct = Async.CancellationToken    
    use cts = CancellationTokenSource.CreateLinkedTokenSource ct
    let tcs = TaskCompletionSource<unit>()    
    let queues = Dictionary<TopicName * Partition, BlockingCollection<Message>>()    
    let close () =
      for kvp in queues do
        try kvp.Value.CompleteAdding() with _ -> ()
    tcs.Task.ContinueWith(fun (_:Task<unit>) -> cts.Cancel()) |> ignore
    let consumePartition (t:TopicName, p:Partition) (buf:BlockingCollection<Message>) = async {      
      try
        use buf = buf
        do!
          buf.GetConsumingEnumerable ()
          |> AsyncSeq.ofSeq
          |> AsyncSeq.bufferByCountAndTime batchSize batchLingerMs
          |> AsyncSeq.iterAsync (fun ms -> handle { ConsumerMessageSet.topic = t ; partition = p ; messages = ms })
      with ex ->        
        tcs.TrySetException ex |> ignore }
    let pollBuf = ResizeArray<_>()
    try
      let mutable m = Unchecked.defaultof<_>
      while (not cts.Token.IsCancellationRequested) do
        if c.Consume(&m, pollTimeoutMs) then
          Message.throwIfError m
          pollBuf.Add m
          while c.Consume(&m, 0) do
            Message.throwIfError m  
            pollBuf.Add m
          pollBuf 
          |> Seq.groupBy (fun m -> m.Topic,m.Partition)
          |> Seq.toArray
          |> Array.Parallel.iter (fun (tp,ms) ->            
            let mutable queue = Unchecked.defaultof<_>
            if not (queues.TryGetValue (tp,&queue)) then
              queue <- new BlockingCollection<_>(batchSize)
              queues.Add (tp,queue)
              Async.Start (consumePartition tp queue, cts.Token)
            for m in ms do
              queue.Add m)
          pollBuf.Clear ()
      if tcs.Task.IsFaulted then
        raise tcs.Task.Exception
    finally
      close () }

  /// Consumes messages, buffers them by time and batchSize and calls the specified handler.
  /// The handler is called sequentially within a partition and in parallel across partitions.
  /// Offsets are commited periodically at the specified interval.
  /// @commitIntervalMs - the offset commit interval.
  /// @pollTimeoutMs - the consumer poll timeout.
  /// @batchLingerMs - the time to wait to form a per-partition batch; when time limit is reached the handler is called.
  /// @batchSize - the per-partition batch size limit; when the limit is reached, the handler is called.
  let consumeCommitInterval
    (c:Consumer) 
    (commitIntervalMs:int)
    (pollTimeoutMs:int)
    (batchLingerMs:int) 
    (batchSize:int) 
    (handle:ConsumerMessageSet -> Async<unit>) : Async<unit> = async {
    let q = OffsetCommitQueue.create c
    let handle ms = async {
      do! handle ms
      let os = ms.messages |> Array.map (fun m -> m.TopicPartitionOffset)
      OffsetCommitQueue.enqueue q os }
    let! consumeProc = Async.StartChild (consume c pollTimeoutMs batchLingerMs batchSize handle)
    let! commitProc = Async.StartChild (OffsetCommitQueue.start q commitIntervalMs)    
    let! _ = consumeProc
    let! _ = commitProc    
    return () }

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
