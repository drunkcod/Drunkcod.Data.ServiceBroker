using System;
using System.IO;

namespace Drunkcod.Data.ServiceBroker
{
	public interface IWorkQueue<T>
	{
		void Post(T item);
		bool Receive(Action<T> handleItem);
	}

	public interface IWorkQueue
	{
		void Post<T>(T item);
		bool Receive(Action<string, object> handleItem);
	}
}
