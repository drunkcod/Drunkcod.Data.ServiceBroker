using System;

namespace Drunkcod.Data.ServiceBroker
{
	public interface IChannel<T>
	{
		void Send(T item);
		bool TryReceive(Action<T> handleItem, TimeSpan timeout);
	}

	public interface IChannel
	{
		void Send(object item);
		bool TryReceive(Action<string, object> handleItem, TimeSpan timeout);
	}

	public static class ChannelExtensions
	{
		public static bool TryReceive(this IChannel self, Action<string, object> handleItem) {
			return self.TryReceive(handleItem, TimeSpan.Zero);
		}

		public static bool TryReceive<T>(this IChannel<T> self, Action<T> handleItem) {
				return self.TryReceive(handleItem, TimeSpan.Zero);
		}
	}
}
