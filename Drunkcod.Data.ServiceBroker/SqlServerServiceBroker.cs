using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;

namespace Drunkcod.Data.ServiceBroker
{
	public class SqlServerServiceBroker
	{
		const string ServiceBrokerEndDialog = "http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog";
		public const string SinkName = "Drunkcod.Data.ServiceBroker.Sink";

		readonly SqlCommander db;
		readonly IMessageSerializer serializer = new JsonMessageSerializer();

		public SqlServerServiceBroker(SqlCommander db) {
			this.db = db;
		}

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
			return OpenQueue(name);
		}

		private ServiceBrokerQueue OpenQueue(string name)
		{
			return new ServiceBrokerQueue(db, name);
		}

		public void DeleteQueue(string name) {
			foreach(var service in new ServiceBrokerQueue(db, name).GetServices())
				db.ExecuteNonQuery($"drop service [{service.Name}]");
			db.ExecuteNonQuery($"drop queue [{name}]");
		}

		public IEnumerable<ServiceBrokerQueue> GetQueues() {
			return db.ExecuteReader(
				"select name from sys.service_queues where is_ms_shipped = 0 and name != @sink_queue",
				x => x.AddWithValue("@sink_queue", SinkName),
				CommandBehavior.SequentialAccess,
				reader => new ServiceBrokerQueue(db, reader.GetString(0))
			);
		} 

		public ServiceBrokerContract CreateContract(string name, params ServiceBrokerMessageType[] messageTypes) {
			return CreateContract(name, messageTypes, new ServiceBrokerMessageType[0]);
		}

		public ServiceBrokerContract CreateContract(string name, 
			IEnumerable<ServiceBrokerMessageType> sentByInitiator,
			IEnumerable<ServiceBrokerMessageType> sentByTarget) {
			var query = new StringBuilder($"if not exists(select null from sys.service_contracts where name = '{name}') create contract [{name}](");
			var sep = string.Empty;
			var target = new HashSet<ServiceBrokerMessageType>(sentByTarget);

			foreach(var item in sentByInitiator) {
				query.AppendFormat("{0}[{1}] sent by {2}", sep, item.Name, target.Remove(item) ? "any" : "initiator");
				sep = ", ";
			}
			foreach(var item in target) {
				query.AppendFormat("{0}[{1}] sent by target", sep, item.Name);
				sep = ", ";
			}
			if(sep == string.Empty)
				throw new InvalidOperationException("Need at least one message type");
			db.ExecuteNonQuery(query.Append(")").ToString());
			return new ServiceBrokerContract(name);
		}

		public IEnumerable<ServiceBrokerContract> GetContracts() {
			return db.ExecuteReader(
@"select name from sys.service_contracts where name not in(
	'DEFAULT', 
	'http://schemas.microsoft.com/SQL/Notifications/PostQueryNotification',
	'http://schemas.microsoft.com/SQL/Notifications/PostEventNotification',
	'http://schemas.microsoft.com/SQL/ServiceBroker/BrokerConfigurationNotice',
	'http://schemas.microsoft.com/SQL/ServiceBroker/ServiceEcho',
	'http://schemas.microsoft.com/SQL/ServiceBroker/ServiceDiagnostic'
)",
				x => { },
				CommandBehavior.SequentialAccess,
				reader => new ServiceBrokerContract(reader.GetString(0)));
		} 

		public void DeleteContract(string name) {
			db.ExecuteNonQuery($"drop contract [{name}]");
        }

		public ServiceBrokerConversation BeginConversation(ServiceBrokerService from, ServiceBrokerService to, ServiceBrokerContract contract) {
			return OpenConversation((Guid)db.ExecuteScalar(
$@"declare @cid uniqueidentifier
begin dialog @cid
from service [{from.Name}]
to service '{to.Name}'
on contract [{contract.Name}]
with encryption = off

select @cid"));
		}

		public ServiceBrokerConversation OpenConversation(Guid conversationHandle) {
			return new ServiceBrokerConversation((x, setup) => db.ExecuteNonQuery(x, setup), conversationHandle);
		}

		public ServiceBrokerConversation GetTargetConversation(ServiceBrokerConversation initiator) {
			return OpenConversation((Guid)db.ExecuteScalar(
$@"select 
	target_handle = target.conversation_handle
from sys.conversation_endpoints initiator
join sys.conversation_endpoints target on initiator.conversation_id = target.conversation_id
where initiator.conversation_handle = @cid
and initiator.conversation_handle != target.conversation_handle", x => x.AddWithValue("@cid", initiator.Handle)));
		}

		public ServiceBrokerService CreateSinkService() {
			if((int)db.ExecuteScalar($"select count(*) from sys.services where name = '{SinkName}'") != 1) {
				var handlerName = SinkName + " Handler";
				if(!db.ObjectExists(handlerName))
					db.ExecuteNonQuery(
$@"create procedure [{handlerName}]
as
	declare @cid uniqueidentifier
	declare @message_type sysname

	begin transaction
		while 1 = 1 begin
			receive top(1)
				@cid = conversation_handle,
				@message_type = message_type_name
			from [{SinkName}]

			if @@rowcount = 0 
				break
			if @message_type = N'{ServiceBrokerEndDialog}'
				end conversation @cid
	end
	commit");
				db.ExecuteNonQuery($"if object_id('[{SinkName}]') is null create queue [{SinkName}] with activation(status = on, procedure_name = [{SinkName} Handler], max_queue_readers = 1, execute as owner)");
				db.ExecuteNonQuery($"create service [{SinkName}] on queue [{SinkName}]");
			}
			return new ServiceBrokerService(SinkName);
		}

		public void Send(IEnumerable<ServiceBrokerConversation> conversations, ServiceBrokerMessageType messageType, byte[] body) {
			var sep = " (";
			var q = new StringBuilder("send on conversation");
			var cmd = db.NewCommand(string.Empty);
			foreach(var item in conversations) {
				var p = cmd.Parameters.AddWithValue("@c" + cmd.Parameters.Count, item.Handle);
				q.AppendFormat("{0}{1}", sep, p.ParameterName);
				sep = ", ";
			}
			q.Append($") message type [{messageType.Name}](@body)");
			cmd.Parameters.AddWithValue("@body", body);
			cmd.CommandText = q.ToString();
			try {
				cmd.Connection.Open();
				cmd.ExecuteNonQuery();
			} finally {
				cmd.Connection.Close();
				cmd.Dispose();
			}
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

		class ServiceBrokerChannel
		{
			readonly ConversationEndpoint endpoint;
			readonly ServiceBrokerQueue queue;
			readonly IMessageSerializer serializer;

			public ServiceBrokerChannel(ConversationEndpoint endpoint, ServiceBrokerQueue queue, IMessageSerializer serializer) {
				this.queue = queue;
				this.endpoint = endpoint;
				this.serializer = serializer;
			}

			public void Send(ServiceBrokerMessageType messageType, object item) {
				var conversation = endpoint.BeginConversation();
				conversation.Send(messageType, serializer.Serialize(item));
			}

			public bool TryReceive(Action<string, object> handleMessage, TimeSpan timeout) {
				return queue.TryReceive((c, type, body) => {
					handleMessage(type.Name, serializer.Deserialize(body, type));
					c.EndConversation();
				}, timeout);
			}

			public bool TryReceive<T>(Action<T> handleMessage, TimeSpan timeout) {
				return queue.TryReceive((c, type, body) => {
					handleMessage(serializer.Deserialize<T>(body));
					c.EndConversation();
				}, timeout);
			}
		}

		class ServiceBrokerTypedChannel<T> : IChannel<T>
		{
			readonly ServiceBrokerChannel channel;
			readonly ServiceBrokerMessageType workItemMessageType;

			public ServiceBrokerTypedChannel(ServiceBrokerChannel channel, ServiceBrokerMessageType workItemMessageType) {
				this.channel = channel;
				this.workItemMessageType = workItemMessageType;
			}

			public void Send(T item) { channel.Send(workItemMessageType, item); }

			public bool TryReceive(Action<T> handleItem, TimeSpan timeout) {
				return channel.TryReceive(handleItem, timeout);
			}
		}

		class ServiceBrokerUntypedChannel : IChannel
		{
			readonly ServiceBrokerChannel channel;
			readonly Type[] supportedTypes;

			public ServiceBrokerUntypedChannel(ServiceBrokerChannel channel, Type[] supporteTypes) {
				this.channel = channel;
				this.supportedTypes = supporteTypes;
			}

			public void Send(object item) {
				try {
					channel.Send(new ServiceBrokerMessageType(item.GetType().FullName), item);
				} catch(SqlException ex) {
					if(!supportedTypes.Contains(item.GetType()))
						throw new InvalidOperationException("Unsupported type for channel", ex);
					throw;
				}
			}

			public bool TryReceive(Action<string,object> handleItem, TimeSpan timeout) {
				return channel.TryReceive(handleItem, timeout);
			}
		}

		public IChannel<T> OpenChannel<T>() {
			var name = typeof(T).FullName;
			var workItemMessageType = CreateMessageType(name);
			return new ServiceBrokerTypedChannel<T>(NewChannel(name, new [] { workItemMessageType }), workItemMessageType);
		}

		public IChannel OpenChannel(string name, params Type[] wantedMessageTypes) {
			var messageTypes = Array.ConvertAll(wantedMessageTypes, x => CreateMessageType(x.FullName));
			return new ServiceBrokerUntypedChannel(NewChannel(name, messageTypes), wantedMessageTypes);
		}

		private ServiceBrokerChannel NewChannel(string name, ServiceBrokerMessageType[] messageTypes) {
			var workerContract = CreateContract(name, messageTypes);
			var workQueue = CreateQueue(name);
			var endpoint = CreateEndpoint(name, workQueue, workerContract);
			var channel = new ServiceBrokerChannel(endpoint, workQueue, serializer);
			return channel;
		}

		class ServiceBrokerConversationChannel : IChannel
		{
			readonly ServiceBrokerConversation conversation;
			readonly ServiceBrokerQueue queue;
			readonly  IMessageSerializer serializer;

			public ServiceBrokerConversationChannel(ServiceBrokerConversation conversation, ServiceBrokerQueue queue, IMessageSerializer serializer) {
				this.conversation = conversation;
				this.queue = queue;
				this.serializer = serializer;
			}

			public void Send(object item) {
				conversation.Send(new ServiceBrokerMessageType(item.GetType().FullName), serializer.Serialize(item));
			}

			public bool TryReceive(Action<string, object> handleItem, TimeSpan timeout) {
				return queue.TryReceive((c, type, body) => {
					handleItem(type.Name, serializer.Deserialize(body, type));
				}, timeout);
			}
		}

		public IChannel OpenConversationChannel(ServiceBrokerConversation conversation) {
			var queue = OpenQueue((string)db.ExecuteScalar(
@"select queues.name
from sys.conversation_endpoints endpoints
join sys.service_queue_usages queue_usages on endpoints.service_id = queue_usages.service_id
join sys.service_queues queues on queues.object_id = queue_usages.service_queue_id
where conversation_handle = @cid", x => x.AddWithValue("@cid", conversation.Handle)));
			return new ServiceBrokerConversationChannel(conversation, queue, serializer);
		}

		private ConversationEndpoint CreateEndpoint(string name, ServiceBrokerQueue queue, ServiceBrokerContract contract) {
			var service = queue.CreateService(name, contract);
			var endpoint = new ConversationEndpoint(this, CreateSinkService(), service, contract);
			return endpoint;
		}
	}
}