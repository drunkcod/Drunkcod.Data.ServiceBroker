open System
open Drunkcod.Data.ServiceBroker

type Message = Message of string

[<EntryPoint>]
let main argv = 
    let worker = MailboxProcessor.Start(fun inbox ->
        let rec loop() = async {
            let! msg = inbox.Receive() 
            match msg with 
            | Message(x) -> printf "%s" x
            
            return! loop()
        }
        loop())

    let broker = SqlServerServiceBroker("Server=.;Integrated Security=SSPI;Initial Catalog=WorkWork");
    let channel = broker.OpenChannel()

    let timeout = TimeSpan.FromMilliseconds(100.)
    let rec loop() = 
        channel.TryReceive((fun x -> worker.Post(Message(x))), timeout) 
        |> ignore
        loop()
    loop()
    0