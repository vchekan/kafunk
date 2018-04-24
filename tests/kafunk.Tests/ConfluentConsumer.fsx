#r "bin/release/net45/confluent.kafka.dll"
#load "Refs.fsx"
#load "Confluent.Kafka.fs"
#time "on"

open System
open System.Text
open System.Collections.Generic
open System.Diagnostics
open System.Threading
open Kafunk
open Refs
open Confluent.Kafka
open Confluent.Kafka.Serialization
open FSharp.Control

let Log = Log.create __SOURCE_FILE__

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"
let group = argiDefault 3 "existential-group"

Log.info "running_consumer|host=%s topic=%s group=%s" host topic group

//let config =
//  [       
//    "bootstrap.servers", box host 
//    //"debug", box "broker,cgrp,topic,fetch"
//    //"debug", box "broker,cgrp,topic"
//    //"debug", box "cgrp"
//    //"linger.ms", box 100
//    "group.id", box group
//    //"max.in.flight.requests.per.connection", box 1
//    "default.topic.config", box <| (dict ["auto.offset.reset", box "earliest"])
//    "enable.auto.commit", box false
//    //"fetch.message.max.bytes", box 10000

//  ] |> dict

let go = async {
  
  let config = 
    Config.SafeConsumer
    |> Config.withBootstrapServers host
    |> Config.withGroupId group
    |> Config.withAutoOffsetReset "earliest"
    |> Config.withFetchMaxBytes 100000
    //|> Config.withConfig "debug" "cgrp"

  let consumer = Consumer.create config
  let commitQueue = OffsetCommitQueue.start consumer 10000

  commitQueue.OnOffsetsCommitted
  |> Event.add (fun m -> 
    Log.info "committed_offsets|error=%O offsets=%s" 
      m.Error (m.Offsets |> Seq.map (fun o -> sprintf "[p=%i o=%i e=%O]" o.Partition o.Offset.Value o.Error) |> String.concat ";"))
  
  consumer.OnLog 
  |> Event.add (fun m -> Log.info "log|leve=%i name=%s facility=%s m=%s" m.Level m.Name m.Facility m.Message)
  
  consumer.OnError 
  |> Event.add (fun m -> Log.info "error|%O" m)
  
  consumer.OnConsumeError 
  |> Event.add (fun m -> Log.info "consumer_error|%O" m)
  
  consumer.OnPartitionsAssigned 
  |> Event.add (fun m -> 
    consumer.Assign m
    Log.info "partitions_assigned|%O" (m |> Seq.map (fun x -> sprintf "p=%i" x.Partition) |> String.concat ";"))

  consumer.OnPartitionsRevoked 
  |> Event.add (fun m -> 
    consumer.Unassign ()
    Log.info "partitions_revoked|%O" (m |> Seq.map (fun x -> sprintf "p=%i" x.Partition) |> String.concat ";"))

  consumer.OnPartitionEOF 
  |> Event.add (fun m -> Log.info "eof|%O" m.Partition)

  consumer.OnOffsetsCommitted 
  |> Event.add (fun m -> 
    Log.info "committed_offsets|error=%O offsets=%s" 
      m.Error (m.Offsets |> Seq.map (fun o -> sprintf "[p=%i o=%i e=%O]" o.Partition o.Offset.Value o.Error) |> String.concat ";"))
 
  consumer.Subscribe topic


  let handle (m:Message) = async {
    //Log.info "handing message|p=%i key=%s" m.Partition (Encoding.UTF8.GetString m.Key)
    return () }

  let handleBatch (ms:ConsumerMessageSet) = async {
    Log.info "handling_batch|size=%i partition=%i" ms.messages.Length ms.partition
    //let! _ = consumer.CommitAsync(ms.messages.[ms.messages.Length - 1]) |> Async.AwaitTask
    //failwithf "test error"
    //do! Async.Sleep 100000
    return () }

  use counter = Metrics.counter Log 5000

  //let handle = 
  //  handle
  //  |> Metrics.throughputAsyncTo counter (fun _ -> 1)

  let handleBatch = 
    handleBatch
    |> Metrics.throughputAsyncTo counter (fun (ms,_) -> ms.messages.Length)

  //do!
  //  Consumer.streamBuffered consumer 1000 1000
  //  |> AsyncSeq.iterAsync handleBatch

  //do! Consumer.consume consumer 1000 1000 1000 handleBatch
  //do! Consumer.consumeCommitInterval consumer 10000 1000 1000 1000 handleBatch
  do! Consumer.consumeCommitQueue consumer commitQueue 1000 1000 1000 handleBatch

  //while true do
  //  let mutable m = Unchecked.defaultof<_>
  //  if consumer.Consume(&m, 1000) then
  //    do! handle m
  //  else
  //    Log.info "skipped"

  //let! _ = Async.StartChild (async {
  //  while true do
  //    consumer.Poll (1000) })

  //do!
  //  consumer.OnMessage
  //  |> AsyncSeq.ofObservableBuffered
  //  |> AsyncSeq.iterAsync handle

  return ()
}

try Async.RunSynchronously go
with ex -> Log.error "ERROR|%O" ex

printfn "DONE"