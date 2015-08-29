using System;

namespace Drunkcod.Data.ServiceBroker
{
	public class MessageEventArgs<T> : EventArgs
	{
		public readonly T Message;

		public MessageEventArgs(T message) {
			this.Message = message;
		}
	}
}