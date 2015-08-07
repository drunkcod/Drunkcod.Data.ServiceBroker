using System;
using System.IO;
using System.Text;
using Drunkcod.Data.ServiceBroker;

namespace SimpleWorkQueue
{
	class ServiceBrokerWorkQueue
	{
		readonly SqlServerServiceBroker broker;
		readonly ServiceBrokerQueue workQueue;
		readonly ServiceBrokerMessageType workItemMessageType;
		readonly ServiceBrokerService sinkService;
		readonly ServiceBrokerService workerService;
		readonly ServiceBrokerContract workQueueItems;

		public ServiceBrokerWorkQueue(SqlServerServiceBroker broker, string messageType) {
			this.broker = broker;
			broker.EnableBroker();
			sinkService = broker.CreateSinkService();

			workItemMessageType = broker.CreateMessageType(messageType);
			workQueue = broker.CreateQueue(messageType);
			workQueueItems = broker.CreateContract(messageType, workItemMessageType);
			workerService = workQueue.CreateService(messageType, workQueueItems);
		}

		public void Post(string workItem) {
			var conversation = broker.BeginConversation(sinkService, workerService, workQueueItems);
			conversation.Send(workItemMessageType, Encoding.UTF8.GetBytes(workItem));
		}

		public bool Receive(Action<string> handleWork) {
			return workQueue.Receive((c, type, body) => {
				handleWork(new StreamReader(body).ReadToEnd());
				c.EndConversation();
			});
		}
	}

	class Program
	{
		static void Main(string[] args) {
			var workQueue = new ServiceBrokerWorkQueue(new SqlServerServiceBroker("Server=.;Integrated Security=SSPI;Initial Catalog=WorkWork"), "WorkItem");

			workQueue.Post("Hello World!");
			workQueue.Post("Hello World!!");

			while(workQueue.Receive(Console.WriteLine))
				;
		}
	}
}
