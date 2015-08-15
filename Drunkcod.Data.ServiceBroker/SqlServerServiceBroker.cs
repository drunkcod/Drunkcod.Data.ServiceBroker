using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Text;
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
		readonly JsonSerializer serializer = new JsonSerializer();
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

		public ServiceBrokerContract CreateContract(string name, IEnumerable<ServiceBrokerMessageType> messageTypes) {
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
	declare @message_type sysname

	begin transaction
		while 1 = 1 begin
			receive top(1)
				@cid = conversation_handle,
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
			static readonly Encoding Utf8NoBom = new UTF8Encoding(false);
			readonly JsonSerializer serializer;
			readonly ConversationEndpoint endpoint;
			protected readonly ServiceBrokerQueue Queue;

			protected ServiceBrokerChannelBase(ConversationEndpoint endpoint, ServiceBrokerQueue queue, JsonSerializer serializer) {
				this.Queue = queue;
				this.endpoint = endpoint;
				this.serializer = serializer;
			}

			protected void Send(ServiceBrokerMessageType messageType, Stream body) {
				var conversation = endpoint.BeginConversation();
				conversation.Send(messageType, body);
			}

			protected Stream Serialize(object item) {
				var body = new MemoryStream();
				using(var writer = new StreamWriter(body, Utf8NoBom, 512, true))
					serializer.Serialize(writer, item);
				body.Position = 0;
				return body;
			}

			protected T Deserialize<T>(Stream body) {
				using(var reader = new StreamReader(body, Utf8NoBom))
				using(var json = new JsonTextReader(reader))
					return serializer.Deserialize<T>(json);
			}

			protected object Deserialize(Stream body, Type type) {
				using(var reader = new StreamReader(body, Utf8NoBom))
				using(var json = new JsonTextReader(reader))
					return serializer.Deserialize(json, type);
			}
		}

		class ServiceBrokerTypedChannel<T> : ServiceBrokerChannelBase, IChannel<T>
		{
			readonly ServiceBrokerMessageType workItemMessageType;

			public ServiceBrokerTypedChannel(
				ConversationEndpoint endpoint, 
				ServiceBrokerQueue workQueue, 
				ServiceBrokerMessageType workItemMessageType,
				JsonSerializer serializer) : base(endpoint, workQueue, serializer) {
				this.workItemMessageType = workItemMessageType;
			}

			public void Send(T item) { Send(workItemMessageType, Serialize(item)); }

			public bool Receive(Action<T> handleItem) {
				return Queue.Receive((c, type, body) => {
					handleItem(Deserialize<T>(body));
					c.EndConversation();
				}, TimeSpan.Zero);
			}
		}

		class ServiceBrokerUntypedChannel : ServiceBrokerChannelBase, IChannel
		{
			readonly Type[] supportedTypes;
			public ServiceBrokerUntypedChannel(
				ConversationEndpoint endpoint, 
				ServiceBrokerQueue workQueue,
				JsonSerializer serializer, Type[] supporteTypes) : base(endpoint, workQueue, serializer) {
					this.supportedTypes = supporteTypes;
			}

			public void Send(object item) {
				try {
					Send(new ServiceBrokerMessageType(item.GetType().FullName), Serialize(item));
				} catch(SqlException ex) {
					if(!supportedTypes.Contains(item.GetType()))
						throw new InvalidOperationException("Unsupported type for channel", ex);
					throw;
				}
			}

			public bool Receive(Action<string,object> handleItem) {
				return Queue.Receive((c, type, body) => {
					handleItem(type.Name, Deserialize(body, Type.GetType(type.Name)));
					c.EndConversation();
				}, TimeSpan.Zero);
			}
		}

		public IChannel<T> OpenChannel<T>() {
			var messageType = typeof(T).FullName;
			var workItemMessageType = CreateMessageType(messageType);
			var workerContract = CreateContract(messageType, new[] { workItemMessageType });
			var workQueue = CreateQueue(messageType);
			var endpoint = CreateEndpoint(messageType, workQueue, workerContract);
			return new ServiceBrokerTypedChannel<T>(endpoint, workQueue, workItemMessageType, serializer);
		}

		public IChannel OpenChannel(string name, params Type[] wantedMessageTypes) {
			var messageTypes = Array.ConvertAll(wantedMessageTypes, x => CreateMessageType(x.FullName));
			var workerContract = CreateContract(name, messageTypes);
			var workQueue = CreateQueue(name);
			var endpoint = CreateEndpoint(name, workQueue, workerContract);
			return new ServiceBrokerUntypedChannel(endpoint, workQueue, serializer, wantedMessageTypes);
		}

		private ConversationEndpoint CreateEndpoint(string name, ServiceBrokerQueue workQueue, ServiceBrokerContract workerContract) {
			var workerService = workQueue.CreateService(name, workerContract);
			var endpoint = new ConversationEndpoint(this, CreateSinkService(), workerService, workerContract);
			return endpoint;
		}
	}
}