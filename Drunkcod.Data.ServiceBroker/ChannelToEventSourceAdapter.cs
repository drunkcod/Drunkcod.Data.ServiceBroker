using System;
using System.IO;

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
				OnMessage.InvokeSafe(this, new MessageEventArgs<T>(x), ex => OnError.InvokeSafe(this, new ErrorEventArgs(ex), _ => { }));
			}, timeout);
		}
		 
		public event EventHandler<MessageEventArgs<T>>  OnMessage;
		public event EventHandler<ErrorEventArgs> OnError; 
	}

	static class EventHandlerExtensions
	{
		public static void InvokeSafe<T>(this EventHandler<T> self, object sender, T args, Action<Exception> onError) where T : EventArgs {
			if(self == null)
				return;
			foreach(EventHandler<T> handler in self.GetInvocationList())
				try { handler(sender, args); }
				catch(Exception ex) { onError(ex); }
		}  
	}
}