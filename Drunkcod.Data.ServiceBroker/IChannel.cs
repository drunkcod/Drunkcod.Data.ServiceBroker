using System;

namespace Drunkcod.Data.ServiceBroker
{
	public interface IChannel<T>
	{
		void Send(T item);
		bool Receive(Action<T> handleItem);
	}

	public interface IChannel
	{
		void Send(object item);
		bool Receive(Action<string, object> handleItem);
	}
}
