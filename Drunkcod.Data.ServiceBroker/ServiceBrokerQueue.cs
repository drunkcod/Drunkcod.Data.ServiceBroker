using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace Drunkcod.Data.ServiceBroker
{
	public delegate void ServiceBrokerMessageHandler(ServiceBrokerConversation conversation, ServiceBrokerMessageType messageType, Stream body);

	public class ServiceBrokerQueue
	{
		readonly SqlCommander db;
		readonly string receiveAny;
		readonly string receive;

		internal ServiceBrokerQueue(SqlCommander db, string queueName) {
			this.db = db;
			this.Name = queueName;
			this.receiveAny = 
$@"waitfor(receive top(1) 
	conversation_handle,
	message_type_name,
	message_body
from [{queueName}]), timeout @timeout ";
			this.receive = 
$@"waitfor(receive top(1) 
	conversation_handle,
	message_type_name,
	message_body
from [{queueName}]
where conversation_handle = @conversation), timeout @timeout ";
		}

		public string Name { get; }

		public ServiceBrokerService CreateService(string name, ServiceBrokerContract contract) {
			db.ExecuteNonQuery($"if not exists(select null from sys.services where name = '{name}') create service [{name}] on queue [{this.Name}]([{contract.Name}])");
			return new ServiceBrokerService(name);
		}

		public bool TryReceive(ServiceBrokerMessageHandler handler, TimeSpan timeout) {
			var cmd = db.NewCommand(receiveAny);
			cmd.Parameters.AddWithValue("@timeout", (int)timeout.TotalMilliseconds);
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

		public bool TryReceive(ServiceBrokerConversation sourceConversation, ServiceBrokerMessageHandler handler, TimeSpan timeout) {
			var cmd = db.NewCommand(receive);
			cmd.Parameters.AddWithValue("@timeout", (int)timeout.TotalMilliseconds);
			cmd.Parameters.AddWithValue("@conversation", sourceConversation.Handle);
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

		public List<ServiceBrokerQueueMessage> Peek() {
			using(var cmd = db.NewCommand($"select message_type_name from [{Name}] with(nolock)")) {
				cmd.Connection.Open();
				using(var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection)) {
					var messages = new List<ServiceBrokerQueueMessage>();
					while(reader.Read())
						messages.Add(new ServiceBrokerQueueMessage(
							new ServiceBrokerMessageType(reader.GetString(0))
						));
					return messages;
				}
			}
		}

		public ServiceBrokerQueueStatistics GetStatistics() {
			var stats = new ServiceBrokerQueueStatistics();
			foreach(var item in Peek().GroupBy(x => x.MessageType))
				stats.Add(item.Key.Name, new QueueStatisticsRow(
					item.Key, 
					item.Count())
				);
			return stats;
		}
	}
}