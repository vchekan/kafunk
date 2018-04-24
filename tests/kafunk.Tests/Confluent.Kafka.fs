namespace Confluent.Kafka

open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Text
open System.Threading
open System.Threading.Tasks
open Confluent.Kafka
open FSharp.Control

[<AutoOpen>]
module internal Prelude =
  
  let inline isNull (x:obj) = obj.ReferenceEquals (x,null)

  let inline arrayToSegment (xs:'a[]) : ArraySegment<'a> =
    ArraySegment (xs, 0, xs.Length)

  let inline stringToSegment (s:string) : ArraySegment<byte> =
    if isNull s then ArraySegment()
    else Encoding.UTF8.GetBytes s |> arrayToSegment

  type Async with
    
    static member StartWithContinuationsThreadPool (a:Async<'a>, ok:'a -> unit, err:exn -> unit, cnc:OperationCanceledException -> unit, ct:CancellationToken) =
      let a = async {
        do! Async.SwitchToThreadPool ()
        return! a }
      Async.StartWithContinuations (a,ok,err,cnc,ct)

    /// Like Async.Parallel but short-circuits on exceptions/cancellations, escalating and cancelling other computations.
    static member ParallelEscalateOnError (xs:Async<'a> seq) : Async<'a[]> = async {
      let! ct = Async.CancellationToken
      return!
        Async.FromContinuations <| fun (ok,err,cnc) ->
          let mutable completed = 0
          let xs = xs |> Seq.toArray
          let rs = Array.zeroCreate xs.Length
          let cts = CancellationTokenSource.CreateLinkedTokenSource ct
          let ok i a = 
            rs.[i] <- a
            if Interlocked.Increment &completed = xs.Length then
              ok rs
          let err e = 
            cts.Cancel ()
            err e
          let cnc e = 
            cts.Cancel ()
            cnc e
          for i = 0 to xs.Length - 1 do
            if not cts.IsCancellationRequested then
              Async.StartWithContinuationsThreadPool (xs.[i], ok i, err, cnc, cts.Token) }
        
    static member AwaitTaskCancellationAsError (t:Task<'a>) : Async<'a> = async {
      let! ct = Async.CancellationToken
      return!
        Async.FromContinuations <| fun (ok,err,_) ->
          t.ContinueWith ((fun (t:Task<'a>) ->
            if t.IsFaulted then err t.Exception
            elif t.IsCanceled then err (OperationCanceledException("Task wrapped with Async has been cancelled."))
            elif t.IsCompleted then ok t.Result
            else failwith "unreachable"), ct) |> ignore }

/// Operations on messages.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module Message =
  
  let internal errorMessage (m:Message) (e:Error) =
    sprintf "message_error|topic=%s partition=%i offset=%i code=%O reason=%s" m.Topic m.Partition m.Offset.Value e.Code e.Reason

  let internal errorException (m:Message) (e:Error) =
    exn(errorMessage m e)

  /// Throws an exception if the message has an error.
  let throwIfError (m:Message) =
    if m.Error.HasError then
      raise (errorException m m.Error)

  /// Returns the message key as a UTF8 string.
  let keyString (m:Message) = Encoding.UTF8.GetString m.Key

  /// Returns the message value as a UTF8 string.
  let valueString (m:Message) = Encoding.UTF8.GetString m.Value

///// Consumer offset reset.
//type AutoOffsetReset =
//  | Error
//  | Earliest
//  | Latest
//  with
//    override __.ToString () =
//      match __ with
//      | Error -> "error"
//      | Earliest -> "earliest"
//      | Latest -> "latest"

///// Producer compression codec.
//type CompressionCodec =
//  | NoCompression
//  | GZip
//  | Snappy
//  | LZ4
//  with  
//    override __.ToString () =
//      match __ with
//      | NoCompression -> "none"
//      | GZip -> "gzip"
//      | Snappy -> "snappy"
//      | LZ4 -> "lz4"

///// Producer required acks.
//and RequiredAcks =
//  | AllInSyncAck
//  | LocalAck
//  | NoAck
//  with 
//    override __.ToString () =
//      match __ with
//      | AllInSyncAck -> "all"
//      | LocalAck -> "local"
//      | NoAck -> "none"

/// Kafka client configuration.
type Config = {
  config : Map<string, obj>
} with
  
  /// Empty configuration.
  static member Empty = { config = Map.empty }
      
  static member withConfig<'a> (n:string) (v:'a) (c:Config) : Config =
    { c with config = Map.add n (box v) c.config }

  static member withConfigs (xs:(string * obj) seq) (c:Config) : Config =
    { c with config = (c.config,xs) ||> Seq.fold (fun c (k,v) -> Map.add k v c) }

  static member withConfigKvps (xs:KeyValuePair<string, obj> seq) (c:Config) : Config =
    c |> Config.withConfigs (xs |> Seq.map (fun kvp -> kvp.Key,kvp.Value))

  static member ofKvps (xs:KeyValuePair<string, obj> seq) =
    Config.Empty |> Config.withConfigKvps xs

  static member removeConfig (n:string) (c:Config) =
    { c with config = Map.remove n c.config }

  static member withConfigMap (n:string) (vs:(string * obj) seq) (c:Config) : Config =
    { c with
          config =
            match c.config |> Map.tryFind n with
            | Some x -> 
              let x = x :?> IDictionary<string, obj> |> Seq.map (fun kvp -> kvp.Key,kvp.Value) |> Map.ofSeq
              let x = (x,vs) ||> Seq.fold (fun x (k,v) -> Map.add k v x) |> Map.toSeq |> dict
              c.config |> Map. add n (box x)
            | None -> 
              c.config |> Map.add n (box (dict vs)) }

  /// Returns the configuration.
  static member toConfig (c:Config) = 
    c.config |> Map.toSeq |> dict
  
  /// Safe consumer configuration.
  /// - enable.auto.commit = false
  /// - auto.offset.reset = error
  static member SafeConsumer = 
    Config.Empty
    |> Config.withEnableAutoCommit false
    |> Config.withAutoOffsetReset "error"

  /// Safe producer configuration.
  /// - max.inflight = 1
  /// - acks = all
  static member SafeProducer = 
    Config.Empty
    |> Config.withMaxInFlight 1
    |> Config.withRequiredAcks "all"
    |> Config.withConfig "produce.offset.report" true

  static member withBootstrapServers = Config.withConfig<string> "bootstrap.servers"
  
  static member withClientId = Config.withConfig<string> "client.id"  
  static member withMaxInFlight = Config.withConfig<int> "max.in.flight.requests.per.connection"
  
  static member withEnableAutoCommit = Config.withConfig<bool> "enable.auto.commit"
  static member withAutoOffsetReset x = Config.withConfigMap "default.topic.config" [ "auto.offset.reset",box x ]
  static member withGroupId = Config.withConfig<string> "group.id"
  static member withFetchMaxBytes = Config.withConfig<int> "fetch.message.max.bytes"
  static member withFetchMinBytes = Config.withConfig<int> "fetch.min.bytes"
  static member withFetchMaxWaitMs = Config.withConfig<int> "fetch.wait.max.ms"
  static member withCheckCrc = Config.withConfig<bool> "check.crcs"
  static member withHeartbeatInterval = Config.withConfig<int> "heartbeat.interval.ms"
  static member withSessionTimeout = Config.withConfig<int> "session.timeout.ms"

  static member withLingerMs = Config.withConfig<int> "linger.ms"
  static member withRequiredAcks x c = Config.withConfig<string> "acks" x c
  static member withBatchNumMessages = Config.withConfig<int> "batch.num.messages"
  static member withCompression = Config.withConfig<string> "compression"
  static member withRequestTimeoutMs = Config.withConfig<int> "request.timeout.ms"
  static member withPartitioner = Config.withConfig<string> "partitioner"
  static member withMessageSendMaxRetries = Config.withConfig<int> "message.send.max.retries"

/// Operations on producers.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module Producer =
  
  /// Creates a producer.
  let create (config:Config) =
    let producer = new Producer(Config.toConfig config, false, false)
    producer    

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

  /// Produces a batch of messages sequentially.
  /// Short-circuits on message error.
  let produceBatched (p:Producer) (topic:string) (ms:(ArraySegment<byte> * ArraySegment<byte>)[]) : Async<Message[]> =
    produceBatchedInternal p topic true ms

  /// Produces a batch of messages sequentially.
  /// Short-circuits on message error.
  let produceBatchedBytes (p:Producer) (topic:string) (ms:(byte[] * byte[])[]) : Async<Message[]> =
    let ms = ms |> Array.map (fun (k,v) -> arrayToSegment k, arrayToSegment v)
    produceBatched p topic ms

  /// Produces a batch of messages sequentially.
  /// Short-circuits on message error.
  let produceBatchedString (p:Producer) (topic:string) (ms:(string * string)[]) : Async<Message[]> =
    let ms = ms |> Array.map (fun (k,v) -> stringToSegment k, stringToSegment v)
    produceBatched p topic ms

/// A set of messages from a single partition.
type ConsumerMessageSet = {
  topic : string
  partition : int
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
    
  let cts = new CancellationTokenSource()
  let mutable task : Task<unit> = Unchecked.defaultof<_>
  let mbOffsets : MailboxProcessor<TopicPartitionOffset[]> = MailboxProcessor.Start ((fun _ -> async.Return()), cts.Token)
  let mbFlushes : MailboxProcessor<TaskCompletionSource<unit>> = MailboxProcessor.Start ((fun _ -> async.Return()), cts.Token)
  let committed : Event<CommittedOffsets> = new Event<_>()
  
  let mbStream (mb:MailboxProcessor<'a>) : AsyncSeq<'a> = 
    AsyncSeq.replicateInfiniteAsync (async { return! mb.Receive() })
  
  let mergeChoice5 (s1:AsyncSeq<'a>) (s2:AsyncSeq<'b>) (s3:AsyncSeq<'c>) (s4:AsyncSeq<'d>) (s5:AsyncSeq<'e>) : AsyncSeq<Choice<'a, 'b, 'c, 'd, 'e>> =
    AsyncSeq.mergeAll [ 
      s1 |> AsyncSeq.map Choice1Of5
      s2 |> AsyncSeq.map Choice2Of5
      s3 |> AsyncSeq.map Choice3Of5
      s4 |> AsyncSeq.map Choice4Of5
      s5 |> AsyncSeq.map Choice5Of5 ]
  
  /// The Task representing the offset flush process.
  member __.Task = 
    __.EnsureStarted ()
    task

  /// Event triggered when enqueued offsets are committed.
  member __.OnOffsetsCommitted = committed.Publish

  /// Starts the offset commit queue process.
  member __.StartInternal (commitIntervalMs:int) = async {
       
    // commits accumulated offsets
    let commit (s:OffsetCommitQueueState) = async {
      let offsets = OffsetCommitQueueState.getOffsets s
      if offsets.Length > 0 then
        //printfn "committing_offsets|%A" offsets
        let! res = consumer.CommitAsync offsets |> Async.AwaitTask
        if (res.Error.HasError) then
          failwithf "offset_commit_error|code=%O reason=%s" res.Error.Code res.Error.Reason
        committed.Trigger res
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

  member internal __.Start (commitIntervalMs:int) =
    lock cts (fun () ->
      if isNull task then
        task <- Async.StartAsTask (__.StartInternal commitIntervalMs, cancellationToken = cts.Token)
      else
        failwith "already_started")

  member __.Stop () =
    cts.Cancel ()

  member private __.EnsureStarted () =
    if cts.IsCancellationRequested then
      failwith "must be started"

  /// Enqueues offsets to be committed asynchronously.
  member __.Enqueue os = 
    __.EnsureStarted ()
    mbOffsets.Post os

  member __.Flush () : Async<unit> = async {
    __.EnsureStarted ()
    let tcs = TaskCompletionSource<unit>()
    mbFlushes.Post tcs
    return! tcs.Task |> Async.AwaitTask }

  interface IDisposable with
    member __.Dispose () = __.Stop ()

/// Operations of the offset commit queue.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module OffsetCommitQueue =
  
  /// Creates an offset commit queue.
  let start (c:Consumer) (commitIntervalMs:int) = 
    let q = new OffsetCommitQueue(c)
    q.Start commitIntervalMs
    q
    
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
  let create (config:Config) =
    let consumer = new Consumer(Config.toConfig config)
    consumer.OnPartitionsAssigned |> Event.add (fun m -> consumer.Assign m)
    consumer.OnPartitionsRevoked |> Event.add (fun _ -> consumer.Unassign ())
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
    let queues = ConcurrentDictionary<string * int, BlockingCollection<Message>>()    
    let close () =
      for kvp in queues do
        try kvp.Value.CompleteAdding() with _ -> ()
    tcs.Task.ContinueWith(fun (_:Task<unit>) -> cts.Cancel()) |> ignore
    let consumePartition (t:string, p:int) (queue:BlockingCollection<Message>) = async {      
      try
        use queue = queue
        do!
          queue.GetConsumingEnumerable ()
          |> AsyncSeq.ofSeq
          |> AsyncSeq.bufferByCountAndTime batchSize batchLingerMs
          |> AsyncSeq.iterAsync (fun ms -> handle { ConsumerMessageSet.topic = t ; partition = p ; messages = ms })
      with ex ->
        tcs.TrySetException ex |> ignore }    
    let poll = async {
      try
        let buf = ResizeArray<_>()
        let mutable m = Unchecked.defaultof<_>
        while (not cts.Token.IsCancellationRequested) do
          if c.Consume(&m, pollTimeoutMs) then
            Message.throwIfError m
            buf.Add m
            while c.Consume(&m, 0) && (not cts.Token.IsCancellationRequested) do
              Message.throwIfError m  
              buf.Add m
            buf 
            |> Seq.groupBy (fun m -> m.Topic,m.Partition)
            |> Seq.toArray
            |> Array.Parallel.iter (fun (tp,ms) ->
              let mutable queue = Unchecked.defaultof<_>
              if not (queues.TryGetValue (tp,&queue)) then
                queue <- new BlockingCollection<_>(batchSize)
                queues.TryAdd (tp,queue) |> ignore
                Async.Start (consumePartition tp queue, cts.Token)
              for m in ms do
                queue.Add m)
            buf.Clear ()
      with ex ->
        tcs.TrySetException ex |> ignore }
    Async.Start (poll, cts.Token)
    try return! tcs.Task |> Async.AwaitTaskCancellationAsError finally close () }

  /// Consumes messages, buffers them by time and batchSize and calls the specified handler.
  /// The handler is called sequentially within a partition and in parallel across partitions.
  /// Offsets are commited periodically at the specified interval.
  /// @q - the offset commit queue.
  /// @pollTimeoutMs - the consumer poll timeout.
  /// @batchLingerMs - the time to wait to form a per-partition batch; when time limit is reached the handler is called.
  /// @batchSize - the per-partition batch size limit; when the limit is reached, the handler is called.
  let consumeCommitQueue
    (c:Consumer)
    (q:OffsetCommitQueue)
    (pollTimeoutMs:int)
    (batchLingerMs:int) 
    (batchSize:int) 
    (handle:ConsumerMessageSet -> Async<unit>) : Async<unit> = async {
    let handle ms = async {
      do! handle ms
      let os = ms.messages |> Array.map (fun m -> m.TopicPartitionOffset)
      OffsetCommitQueue.enqueue q os }
    let consume = consume c pollTimeoutMs batchLingerMs batchSize handle
    return! Async.ParallelEscalateOnError [ consume ; Async.AwaitTask q.Task ] |> Async.Ignore }

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
    (handle:ConsumerMessageSet -> Async<unit>) : Async<unit> =
    let q = OffsetCommitQueue.start c commitIntervalMs
    consumeCommitQueue c q pollTimeoutMs batchLingerMs batchSize handle

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
