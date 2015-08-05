using System;

namespace Drunkcod.Data.ServiceBroker
{
	public struct ServiceBrokerMessageType
	{
		public readonly string Name;
		internal ServiceBrokerMessageType(string name) { this.Name = name; }
	}

	public struct ServiceBrokerContract
	{
		public readonly string Name;
		internal ServiceBrokerContract(string name) { this.Name = name; }
	}

	public struct ServiceBrokerService
	{
		public readonly string Name;
		internal ServiceBrokerService(string name) { this.Name = name; }
	}

	public class SqlServerServiceBroker
	{
		const string SinkServiceName = "Drunkcod.Data.ServiceBroker.SinkService";
		readonly SqlCommander db;

		public SqlServerServiceBroker(string connectionString) {
			this.db = new SqlCommander(connectionString);
		}

		public void EnableBroker() {
			ExecuteNonQuery("if not exists(select null from sys.databases where database_id = db_id() and is_broker_enabled = 1) begin declare @sql nvarchar(max) = N'alter database [' + db_name() + '] set enable_broker with rollback immediate'; exec sp_executesql @sql end");
		}

		public ServiceBrokerMessageType CreateMessageType(string name) {
			ExecuteNonQuery($"if not exists(select null from sys.service_message_types where name = '{name}') create message type [{name}] validation = none");
			return new ServiceBrokerMessageType(name);
		}

		public ServiceBrokerQueue CreateQueue(string name) {
			ExecuteNonQuery($"if not exists(select null from sys.service_queues where name = '{name}') create queue [{name}]");

			return new ServiceBrokerQueue(db, name);
		}

		public ServiceBrokerContract CreateContract(string name, ServiceBrokerMessageType messageType) {
			ExecuteNonQuery($"if not exists(select null from sys.service_contracts where name = '{name}') create contract [{name}]({messageType.Name} sent by initiator)");
			return new ServiceBrokerContract(name);
		}

		public ServiceBrokerConversation BeginConversation(ServiceBrokerService from, ServiceBrokerService to, ServiceBrokerContract contract) {
			return new ServiceBrokerConversation(db, (Guid)db.ExecuteScalar(
				$@"declare @cid uniqueidentifier
begin dialog @cid
from service [{from.Name}]
to service '{to.Name}'
on contract [{contract.Name}]
with encryption = off

select @cid"), false);
		}

		public ServiceBrokerConversation OpenConversation(Guid conversationHandle) {
			return new ServiceBrokerConversation(db, conversationHandle, false);
		}

		public ServiceBrokerService CreateSinkService() {
			if((int)db.ExecuteScalar($"select count(*) from sys.services where name = '{SinkServiceName}'") != 1) { 

				if(db.ExecuteScalar("select object_id('handle_sink')") is DBNull)
					ExecuteNonQuery(
@"create procedure handle_sink
as
	declare @cid uniqueidentifier
	declare @message_body varbinary(max)
	declare @message_type sysname

	begin transaction
		while 1 = 1 begin
			receive top(1)
				@cid = conversation_handle,
				@message_body = message_body,
				@message_type = message_type_name
			from SinkQueue

			if @@rowcount = 0 
				break
			if @message_type = N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'
				end conversation @cid
	end
	commit");
				ExecuteNonQuery("if object_id('SinkQueue') is null create queue SinkQueue with activation(status = on, procedure_name = handle_sink, max_queue_readers = 1, execute as owner)");
				ExecuteNonQuery($"create service [{SinkServiceName}] on queue SinkQueue");
			}
			return new ServiceBrokerService(SinkServiceName);
		}

		void ExecuteNonQuery(string query) {
			db.ExecuteNonQuery(query, _ => { });
		}
	}
}