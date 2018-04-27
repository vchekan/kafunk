module ConfluentWrapperTest

open Confluent.Kafka
open NUnit.Framework

[<Test>]
[<Category("Confluent Client")>] 
let ``Producer and consumer work``() = 
    let producer = 
        Config.Producer.safe
        |> Config.clientId "test-client"
        |> Producer.create
    do ()


