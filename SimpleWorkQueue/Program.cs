using System;
using System.IO;
using System.Text;
using Drunkcod.Data.ServiceBroker;
using Newtonsoft.Json;

namespace SimpleWorkQueue
{

	class Program
	{
		static void Main(string[] args) {
			var workQueue = new SqlServerServiceBroker("Server=.;Integrated Security=SSPI;Initial Catalog=WorkWork")
				.OpenWorkQueue<string>();

			workQueue.Post("Hello World!");
			workQueue.Post("Hello World!!");

			while(workQueue.Receive(Console.WriteLine))
				;
		}
	}
}
