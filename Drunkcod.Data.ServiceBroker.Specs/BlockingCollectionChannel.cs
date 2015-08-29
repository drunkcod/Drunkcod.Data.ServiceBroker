using System;
using System.Collections.Concurrent;

namespace Drunkcod.Data.ServiceBroker.Specs
{
	class BlockingCollectionChannel<T> : IChannel<T>
	{
		readonly BlockingCollection<T> messages = new BlockingCollection<T>(); 

		public void Send(T item) {
			messages.Add(item);
		}

		public bool TryReceive(Action<T> handleItem, TimeSpan timeout) {
			T found;
			if(!messages.TryTake(out found, timeout))
				return false;
			handleItem(found);
			return true;
		}
	}
}