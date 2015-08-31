using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Drunkcod.Data.ServiceBroker;
using Newtonsoft.Json;
using SimpleAgents;

namespace SimpleWorkQueue
{
	class Program
	{
		static void Main(string[] args) {
			var broker = new SqlServerServiceBroker("Server=.;Integrated Security=SSPI;Initial Catalog=WorkWork");
/*
			var workQueue = broker.OpenChannel<string>();
			Task.Factory.StartNew(() => {
				for(var i = 0; i != 10; ++i) {
					workQueue.Send(i.ToString());
					Thread.Sleep(250);
				}
				workQueue.Send("Bye.");
			}, TaskCreationOptions.LongRunning);

			for(var done = false; !done;) {
				workQueue.TryReceive(x => {
					if(x == "Bye.")
						done = true;
					Console.WriteLine(x);
				});
			}
*/
			var q2 = broker.OpenChannel<Message>();
			for(;;) 
				q2.Send(Message.NewMessage(Console.ReadLine()));

		}
	}
}
