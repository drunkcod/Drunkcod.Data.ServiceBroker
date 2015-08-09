using System;
using System.IO;
using Newtonsoft.Json;

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
		const string ServiceBrokerEndDialog = "http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog";
		const string SinkServiceName = "Drunkcod.Data.ServiceBroker.SinkService";
		readonly SqlCommander db;

		public SqlServerServiceBroker(string connectionString) {
			this.db = new SqlCommander(connectionString);
		}

		public void EnableBroker() {
			db.ExecuteNonQuery("if not exists(select null from sys.databases where database_id = db_id() and is_broker_enabled = 1) begin declare @sql nvarchar(max) = N'alter database [' + db_name() + '] set enable_broker with rollback immediate'; exec sp_executesql @sql end");
		}

		public ServiceBrokerMessageType CreateMessageType(string name) {
			db.ExecuteNonQuery($"if not exists(select null from sys.service_message_types where name = '{name}') create message type [{name}] validation = none");
			return new ServiceBrokerMessageType(name);
		}

		public ServiceBrokerQueue CreateQueue(string name) {
			db.ExecuteNonQuery($"if object_id('[{name}]') is null create queue [{name}]");

			return new ServiceBrokerQueue(db, name);
		}

		public ServiceBrokerContract CreateContract(string name, ServiceBrokerMessageType messageType) {
			db.ExecuteNonQuery($"if not exists(select null from sys.service_contracts where name = '{name}') create contract [{name}]([{messageType.Name}] sent by initiator)");
			return new ServiceBrokerContract(name);
		}

		public ServiceBrokerConversation BeginConversation(ServiceBrokerService from, ServiceBrokerService to, ServiceBrokerContract contract) {
			return new ServiceBrokerConversation(db.ExecuteNonQuery, (Guid)db.ExecuteScalar(
				$@"declare @cid uniqueidentifier
begin dialog @cid
from service [{from.Name}]
to service '{to.Name}'
on contract [{contract.Name}]
with encryption = off

select @cid"));
		}

		public ServiceBrokerConversation OpenConversation(Guid conversationHandle) {
			return new ServiceBrokerConversation(db.ExecuteNonQuery, conversationHandle);
		}

		public ServiceBrokerService CreateSinkService() {
			if((int)db.ExecuteScalar($"select count(*) from sys.services where name = '{SinkServiceName}'") != 1) { 
				if(db.ExecuteScalar($"select object_id('[{SinkServiceName} Handler]')") is DBNull)
					db.ExecuteNonQuery(
$@"create procedure [{SinkServiceName} Handler]
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
			if @message_type = N'{ServiceBrokerEndDialog}'
				end conversation @cid
	end
	commit");
				db.ExecuteNonQuery($"if object_id('SinkQueue') is null create queue SinkQueue with activation(status = on, procedure_name = [{SinkServiceName} Handler], max_queue_readers = 1, execute as owner)");
				db.ExecuteNonQuery($"create service [{SinkServiceName}] on queue SinkQueue");
			}
			return new ServiceBrokerService(SinkServiceName);
		}

		class TypedServiceBrokerQueue<T> : IWorkQueue<T>
		{
			readonly SqlServerServiceBroker broker;
			readonly ServiceBrokerQueue workQueue;
			readonly JsonSerializer serializer = new JsonSerializer();
			readonly ServiceBrokerService sinkService;
			readonly ServiceBrokerService workerService;
			readonly ServiceBrokerContract workerContract;
			readonly ServiceBrokerMessageType workItemMessageType;

			public TypedServiceBrokerQueue(SqlServerServiceBroker broker, ServiceBrokerService workerService, ServiceBrokerQueue workQueue, ServiceBrokerContract workerContract, ServiceBrokerMessageType workItemMessageType) {
				this.broker = broker;
				this.sinkService = broker.CreateSinkService();
				this.workerService = workerService;
				this.workQueue = workQueue;
				this.workerContract = workerContract;
				this.workItemMessageType = workItemMessageType;
			}

			public void Post(T item) {
				var conversation = broker.BeginConversation(sinkService, workerService, workerContract);
				var body = new MemoryStream();
				using(var writer = new StreamWriter(body))
					serializer.Serialize(writer, item);
				conversation.Send(workItemMessageType, body.ToArray());
			}

			public bool Receive(Action<T> handleItem) {
				return workQueue.Receive((c, type, body) => {
					using(var reader = new StreamReader(body))
					using(var json = new JsonTextReader(reader))
						handleItem(serializer.Deserialize<T>(json));
					c.EndConversation();
				});
			}
		}

		public IWorkQueue<T> OpenWorkQueue<T>() {
			var messageType = typeof(T).FullName;
			var workItemMessageType = CreateMessageType(messageType);
			var workQueue = CreateQueue(messageType);
			var workerContract = CreateContract(messageType, workItemMessageType);
			var workerService = workQueue.CreateService(messageType, workerContract);
			return new TypedServiceBrokerQueue<T>(this, workerService, workQueue, workerContract, workItemMessageType);
		}
	}
}