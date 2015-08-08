using System;
using System.Collections.Generic;
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
			db.ExecuteNonQuery($"if not exists(select null from sys.services where name = '{name}') create service [{name}] on queue [{this.queueName}]([{contract.Name}])", _ => { });
			return new ServiceBrokerService(name);
		}

		public bool Receive(ServiceBrokerMessageHandler handler) {
			var cmd = db.NewCommand(receive);
			SqlDataReader reader = null;
			var result = false;
			var conversationQueries = new List<Tuple<string, Action<SqlParameterCollection>>>();
			try {
				cmd.Connection.Open();
				cmd.Transaction = cmd.Connection.BeginTransaction();
				reader = cmd.ExecuteReader(CommandBehavior.SingleRow | CommandBehavior.SequentialAccess);
				if (reader.Read()) {
					var conversation = new ServiceBrokerConversation((query, setup) => conversationQueries.Add(Tuple.Create(query, setup)),  reader.GetGuid(0));
					handler(conversation, new ServiceBrokerMessageType(reader.GetString(1)), reader.GetStream(2));
					result = true;
				}
				reader.Close();
				foreach(var item in conversationQueries) {
					cmd.Parameters.Clear();
					cmd.CommandText = item.Item1;
					item.Item2(cmd.Parameters);
					cmd.ExecuteNonQuery();
				}
				cmd.Transaction?.Commit();
				return result;
			} catch {
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