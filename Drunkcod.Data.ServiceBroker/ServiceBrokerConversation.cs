using System;
using System.Data.SqlClient;
using System.IO;

namespace Drunkcod.Data.ServiceBroker
{
	public delegate void NonQueryHandler(string query, Action<SqlParameterCollection> setup); 

	public class ServiceBrokerConversation
	{
		readonly NonQueryHandler db;

		public Guid Handle { get; }

		internal ServiceBrokerConversation(NonQueryHandler db, Guid conversationHandle) {
			this.db = db;
			this.Handle = conversationHandle;
		}

		public void Send(ServiceBrokerMessageType messageType, Stream body) {
			db($"send on conversation @cid message type [{messageType.Name}](@body)", x => {
				x.AddWithValue("@cid", Handle);
				x.AddWithValue("@body", body);
			});
		}

		public void EndConversation() {
			db("end conversation @cid", x => x.AddWithValue("@cid", Handle));
		}
	}
}