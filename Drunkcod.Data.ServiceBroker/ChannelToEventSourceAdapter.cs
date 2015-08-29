using System;

namespace Drunkcod.Data.ServiceBroker
{
	public class ChannelToEventSourceAdapter<T>
	{
		readonly IChannel<T> channel;
		
		public ChannelToEventSourceAdapter(IChannel<T> channel) {
			this.channel = channel;
		}

		public bool Pump(TimeSpan timeout) {
			return channel.TryReceive(x => {
				if(OnMessage == null)
					return;
				var e = new MessageEventArgs<T>(x);
				foreach(EventHandler<MessageEventArgs<T>> handler in OnMessage.GetInvocationList()) {
					try {
						handler(this, e);
					} catch { }
				}
			}, timeout);
		}
		 
		public event EventHandler<MessageEventArgs<T>>  OnMessage;
	}
}