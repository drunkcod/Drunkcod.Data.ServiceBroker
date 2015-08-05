using System;

namespace Drunkcod.Data.ServiceBroker
{
	public class ServiceBrokerConversation
	{
		readonly SqlCommander db;
		readonly Guid conversationHandle;
		readonly bool isInsideTransaction;

		internal ServiceBrokerConversation(SqlCommander db, Guid conversationHandle, bool isInsideTransaction) {
			this.db = db;
			this.conversationHandle = conversationHandle;
			this.isInsideTransaction = isInsideTransaction;
		}

		public bool IsEnded { get; internal set; }

		public void Send(ServiceBrokerMessageType messageType, byte[] body) {
			db.ExecuteNonQuery($"send on conversation @cid message type [{messageType.Name}](@body)", x => {
																											   x.AddWithValue("@cid", conversationHandle);
																											   x.AddWithValue("@body", body);
			});
		}

		public void EndConversation() {
			IsEnded = true;
			if(!isInsideTransaction)
				db.ExecuteNonQuery("end conversation @cid", x => x.AddWithValue("@cid", conversationHandle));
		}
	}
}