using System;
using System.Data.SqlClient;

namespace Drunkcod.Data.ServiceBroker
{
	public delegate void NonQueryHandler(string query, Action<SqlParameterCollection> setup); 

	public class ServiceBrokerConversation
	{
		readonly NonQueryHandler db;
		readonly Guid conversationHandle;

		internal ServiceBrokerConversation(NonQueryHandler db, Guid conversationHandle) {
			this.db = db;
			this.conversationHandle = conversationHandle;
		}

		public void Send(ServiceBrokerMessageType messageType, byte[] body) {
			db($"send on conversation @cid message type [{messageType.Name}](@body)", x => {
				x.AddWithValue("@cid", conversationHandle);
				x.AddWithValue("@body", body);
			});
		}

		public void EndConversation() {
			db("end conversation @cid", x => x.AddWithValue("@cid", conversationHandle));
		}
	}
}