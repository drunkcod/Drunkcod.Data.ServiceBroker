using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Drunkcod.Data.ServiceBroker;
using Newtonsoft.Json;

namespace SimpleWorkQueue
{
	class Program
	{
		static void Main(string[] args) {
			var broker = new SqlServerServiceBroker("Server=.;Integrated Security=SSPI;Initial Catalog=WorkWork");
/*
			var workQueue = broker.OpenWorkQueue<string>();
			Task.Factory.StartNew(() => {
				for(var i = 0; i != 10; ++i) {
					workQueue.Post(i.ToString());
					Thread.Sleep(250);
				}
				workQueue.Post("Bye.");
			}, TaskCreationOptions.LongRunning);

			for(var done = false; !done;) {
				workQueue.Receive(x => {
					if(x == "Bye.")
						done = true;
					Console.WriteLine(x);
				});
			}
*/
			var q2 = broker.OpenWorkQueue("MyQueue", typeof(int), typeof(string));

			q2.Post(42);
			q2.Post("Hello World!");
			while(q2.Receive((type, value) => {
				Console.WriteLine("{0} {1}", type, value);
			}));
		}
	}
}
