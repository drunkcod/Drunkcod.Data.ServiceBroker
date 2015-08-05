using System;
using System.IO;
using System.Text;
using Drunkcod.Data.ServiceBroker;

namespace SimpleWorkQueue
{
	class Program
	{
		static void Main(string[] args) {
			var sbs = new SqlServerServiceBroker("Server=.;Integrated Security=SSPI;Initial Catalog=WorkWork");

			sbs.EnableBroker();
			var workItem = sbs.CreateMessageType("WorkItem");
			var workQueue = sbs.CreateQueue("WorkQueue");

			var workQueueItems = sbs.CreateContract("WorkQueueItems", workItem);

			var sinkService = sbs.CreateSinkService();
			var workerService = workQueue.CreateService("WorkerService", workQueueItems);

			var conversation = sbs.BeginConversation(sinkService, workerService, workQueueItems);
			conversation.Send(workItem, Encoding.UTF8.GetBytes("Hello Service Broker World!"));

			while(workQueue.Receive((c, type, body) => {
				Console.WriteLine("{0}: {1}", type.Name, new StreamReader(body).ReadToEnd());
				c.EndConversation();
			}));
		}
	}
}
