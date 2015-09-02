open Drunkcod.Data.ServiceBroker
open System

let digits n = 
    let rec loop acc = function 0 -> acc | n -> loop (acc + 1) (n / 10)
    loop 1 (n / 10)

[<EntryPoint>]
let main argv = 
    let broker = SqlServerServiceBroker("Server=.;Integrated Security=SSPI;Initial Catalog=WorkWork");

    let displayStats() =
        let theQueues = 
            broker.GetQueues()
            |> Seq.toArray
            |> Array.sortBy (fun x -> x.Name)
    
        let width = theQueues |> Seq.map (fun x -> x.Name.Length) |> Seq.max
        let maxDigits = digits(theQueues |> Seq.map (fun x -> x.Peek().Count) |> Seq.max)
        let rowFormat = String.Format("{{0,-{0}}}│ {{1,{1}}}", width, maxDigits)
        Console.Clear()
        theQueues |> Seq.iter (fun x -> 
            let n = x.Peek().Count
            Console.WriteLine(rowFormat, x.Name, n)
        )
        Console.WriteLine("{0}┘", String('─', width))
        Console.WriteLine(" " + DateTime.Now.ToString())
    
    use timer = new Timers.Timer(1000.0)
    timer.Elapsed.Add(fun _ -> displayStats())
    timer.AutoReset <- true

    displayStats()
    timer.Enabled <- true

    Console.ReadKey()
    0 // return an integer exit code
