using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;

namespace Drunkcod.Data.ServiceBroker
{
	public delegate void ServiceBrokerMessageHandler(ServiceBrokerConversation conversation, ServiceBrokerMessageType messageType, Stream body);

	public class ServiceBrokerQueue
	{
		readonly SqlCommander db;
		readonly string queueName;
		readonly string receive;

		internal ServiceBrokerQueue(SqlCommander db, string queueName) {
			this.db = db;
			this.queueName = queueName;
			this.receive = 
$@"receive top(1) 
	conversation_handle,
	message_type_name,
	message_body
from [{queueName}]";
		}

		public ServiceBrokerService CreateService(string name, ServiceBrokerContract contract) {
			db.ExecuteNonQuery($"if not exists(select null from sys.services where name = '{name}') create service [{name}] on queue {this.queueName}({contract.Name})", _ => { });
			return new ServiceBrokerService(name);
		}

		public bool Receive(ServiceBrokerMessageHandler handler) {
			var cmd = db.NewCommand(receive);
			SqlDataReader reader = null;
			Guid endCid = Guid.Empty;
			var result = false;
			try {
				cmd.Connection.Open();
				cmd.Transaction = cmd.Connection.BeginTransaction();
				reader = cmd.ExecuteReader(CommandBehavior.SingleRow | CommandBehavior.SequentialAccess);
				if (reader.Read()) {
					var cid = reader.GetGuid(0);
					var conversation = new ServiceBrokerConversation(db, cid, true);
					handler(conversation, new ServiceBrokerMessageType(reader.GetString(1)), reader.GetStream(2));
					if(conversation.IsEnded)
						endCid = cid;
					result = true;
				}
				reader.Close();
				if(endCid != Guid.Empty) {
					cmd.Parameters.Clear();
					cmd.CommandText = "end conversation @cid";
					cmd.Parameters.AddWithValue("@cid", endCid);
					cmd.ExecuteNonQuery();
				}
				cmd.Transaction?.Commit();
				return result;
			} catch {
				reader?.Close();
				cmd.Transaction?.Rollback();
				throw;
			} finally {
				reader?.Dispose();
				cmd.Transaction?.Dispose();
				cmd.Connection.Dispose();
				cmd.Dispose();
			}
		}
	}
}