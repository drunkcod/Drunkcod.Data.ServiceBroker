using System;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
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

		public ServiceBrokerContract CreateContract(string name, ServiceBrokerMessageType[] messageTypes) {
			var sentMessages = string.Join(", ", messageTypes.Select(x => $"[{x.Name}] sent by initiator"));
			db.ExecuteNonQuery($"if not exists(select null from sys.service_contracts where name = '{name}') create contract [{name}]({sentMessages})");
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

		struct ConversationEndpoint
		{
			readonly SqlServerServiceBroker broker;
			readonly ServiceBrokerService initiator;
			readonly ServiceBrokerService target;
			readonly ServiceBrokerContract contract;

			public ConversationEndpoint(SqlServerServiceBroker broker, ServiceBrokerService initiator, ServiceBrokerService target, ServiceBrokerContract contract)
			{
				this.broker = broker;
				this.initiator = initiator; 
				this.target = target;
				this.contract = contract;
			}

			[Pure]
			public ServiceBrokerConversation BeginConversation() {
				return broker.BeginConversation(initiator, target, contract);
			}
		}

		class ServiceBrokerChannelBase
		{
			readonly JsonSerializer serializer = new JsonSerializer();
			readonly ServiceBrokerQueue queue;
			readonly ConversationEndpoint endpoint;

			protected ServiceBrokerChannelBase(ConversationEndpoint endpoint, ServiceBrokerQueue queue) {
				this.queue = queue;
				this.endpoint = endpoint;
			}

			protected void Send(ServiceBrokerMessageType messageType, byte[] body) {
				var conversation = endpoint.BeginConversation();
				conversation.Send(messageType, body);
			}

			protected bool Receive(ServiceBrokerMessageHandler handleMessage) {
				return queue.Receive(handleMessage, TimeSpan.Zero);
			}

			protected byte[] Serialize(object item) {
				var body = new MemoryStream();
				using(var writer = new StreamWriter(body))
					serializer.Serialize(writer, item);
				return body.ToArray();
			}

			protected T Deserialize<T>(Stream body) {
				using(var reader = new StreamReader(body))
				using(var json = new JsonTextReader(reader))
					return serializer.Deserialize<T>(json);
			}

			protected object Deserialize(Stream body, Type type) {
				using(var reader = new StreamReader(body))
				using(var json = new JsonTextReader(reader))
					return serializer.Deserialize(json, type);
			}
		}

		class ServiceBrokerTypedChannel<T> : ServiceBrokerChannelBase, IChannel<T>
		{
			readonly ServiceBrokerMessageType workItemMessageType;

			public ServiceBrokerTypedChannel(ConversationEndpoint endpoint, ServiceBrokerQueue workQueue, ServiceBrokerMessageType workItemMessageType) : base(endpoint, workQueue) {
				this.workItemMessageType = workItemMessageType;
			}

			public void Send(T item) { Send(workItemMessageType, Serialize(item)); }

			public bool Receive(Action<T> handleItem) {
				return Receive((c, type, body) => {
					handleItem(Deserialize<T>(body));
					c.EndConversation();
				});
			}
		}

		class ServiceBrokerUntypedChannel : ServiceBrokerChannelBase, IChannel
		{
			public ServiceBrokerUntypedChannel(ConversationEndpoint endpoint, ServiceBrokerQueue workQueue) : base(endpoint, workQueue) {
			}

			public void Send(object item) { Send(new ServiceBrokerMessageType(item.GetType().FullName), Serialize(item)); }

			public bool Receive(Action<string,object> handleItem) {
				return Receive((c, type, body) => {
					handleItem(type.Name, Deserialize(body, Type.GetType(type.Name)));
					c.EndConversation();
				});
			}
		}

		public IChannel<T> OpenWorkQueue<T>() {
			var messageType = typeof(T).FullName;
			var workItemMessageType = CreateMessageType(messageType);
			var workerContract = CreateContract(messageType, workItemMessageType);
			var workQueue = CreateQueue(messageType);
			var workerService = workQueue.CreateService(messageType, workerContract);
			var endpoint = new ConversationEndpoint(this, CreateSinkService(), workerService, workerContract);
			return new ServiceBrokerTypedChannel<T>(endpoint, workQueue, workItemMessageType);
		}

		public IChannel OpenWorkQueue(string name, params Type[] wantedMessageTypes) {
			var messageTypes = Array.ConvertAll(wantedMessageTypes, x => CreateMessageType(x.FullName));
			var workerContract = CreateContract(name, messageTypes);
			var workQueue = CreateQueue(name);
			var workerService = workQueue.CreateService(name, workerContract);
			var endpoint = new ConversationEndpoint(this, CreateSinkService(), workerService, workerContract);
			return new ServiceBrokerUntypedChannel(endpoint, workQueue);
		} 
	}
}