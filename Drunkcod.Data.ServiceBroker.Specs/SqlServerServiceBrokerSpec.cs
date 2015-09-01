using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using Cone;

namespace Drunkcod.Data.ServiceBroker.Specs
{
	[Describe(typeof(SqlServerServiceBroker))]
	public class SqlServerServiceBrokerSpec
	{
		readonly string DbName = $"{typeof(SqlServerServiceBroker).FullName}.Spec";
		static readonly string ConnectionString = Environment.GetEnvironmentVariable("TestDbConnectionString") ?? "Server=.;Integrated Security=SSPI";
		string BrokerConnectionString => ConnectionString + ";Database=" + DbName;
		SqlCommander Db;
		SqlServerServiceBroker Broker;

		[BeforeAll]
		public void create_empty_database() {
			var db = new SqlCommander(ConnectionString);
			db.ExecuteNonQuery($"create database [{DbName}]");
		}

		[AfterAll]
		public void drop_test_database() {
			SqlConnection.ClearAllPools();
			var db = new SqlCommander(ConnectionString);
			db.ExecuteNonQuery($"drop database [{DbName}]");
		}

		[BeforeEach]
		public void enable_broker() {
			Db = new SqlCommander(BrokerConnectionString);
			Broker = new SqlServerServiceBroker(Db);
			Broker.EnableBroker();
		}

		[AfterEach]
		public void drop_user_queues() {
			foreach(var item in Broker.GetQueues())
				Broker.DeleteQueue(item.Name);
			foreach(var item in Broker.GetContracts())
				Broker.DeleteContract(item.Name);
		}

		public void typed_channel_roundtrip() {
			var initiator = Broker.OpenChannel<string>();
			var target = Broker.OpenChannel<string>();

			initiator.Send("Hello World!");
			Check.That(() => target.TryReceive(x => Check.That(() => x == "Hello World!")));
		}

		public void untyped_channel_roundtrip() {
			var initiator = Broker.OpenChannel("MyChannel", typeof(int), typeof(string));
			var target = Broker.OpenChannel("MyChannel", typeof(int), typeof(string));

			initiator.Send("my string");
			Check.That(() => target.TryReceive((t,x) => Check.That(
				() => (string)x == "my string",
				() => t == typeof(string).FullName)));

			initiator.Send(42);
			Check.That(() => target.TryReceive((t,x) => Check.That(
				() => (int)x == 42,
				() => t == typeof(int).FullName)));
		}

		public void posting_unsupported_type_gives_InvalidOperationException() {
			var initiator = Broker.OpenChannel("MyIntChannel", typeof(int));

			Check.Exception<InvalidOperationException>(() => initiator.Send("Hello"));
		}

		public void can_peek_queue() {
			var channel = Broker.OpenChannel<int>();
			var queue = Broker.CreateQueue(typeof(int).FullName);
			channel.Send(1);
			channel.Send(2);

			Action<int> checkOneLeft = _ => Check.That(() => queue.Peek().Count == 1);
			Check.That(
				() => queue.Peek().Count == 2,
				() => channel.TryReceive(checkOneLeft));
		}

		public void peek_queue_fields_contains_message_type() {
			var channel = Broker.OpenChannel<int>();
			var queue = Broker.CreateQueue(typeof(int).FullName);
			channel.Send(42);

			var peekedMessage = queue.Peek().First();
			Check.That(() => peekedMessage.MessageType.Name == typeof(int).FullName);
		}

		public void can_list_user_queues() {
			Broker.OpenChannel<int>();
			Broker.OpenChannel("MyChannel", typeof(int), typeof(string));

			var queues = Broker.GetQueues().OrderBy(x => x.Name).ToList();
			Check.That(
				() => queues.Count == 2,
				() => queues[0].Name == "MyChannel",
				() => queues[1].Name == "System.Int32");
		}

		public void queue_statistics_message_counts() {
			var channel = Broker.OpenChannel("MyChannel", typeof(int), typeof(string));

			channel.Send(1);
			channel.Send(2);
			channel.Send(3);
			channel.Send("Hello World");

			var stats = Broker.CreateQueue("MyChannel").GetStatistics();
			Check.That(
				() => stats.MessageCount == 4,
				() => stats["System.Int32"].Count == 3,
				() => stats["System.String"].Count == 1);
		}

		public void queue_statistics_contains_message_type() {
			var channel = Broker.OpenChannel("MyChannel", typeof(int), typeof(string));

			channel.Send(1);
			channel.Send(2);

			var stats = Broker.CreateQueue("MyChannel").GetStatistics();
			Check.That(
				() => stats.Count() == 1,
				() => stats.First().MessageType.Name == "System.Int32");
		}

		public void conversation_multicast() {
			var myQ = Broker.CreateQueue("MyQueue");
			var myMessage = Broker.CreateMessageType("MyMessage");
			var myContract = Broker.CreateContract("MyContract", myMessage);
			var myService = myQ.CreateService("MyQueue", myContract);

			var c1 = Broker.BeginConversation(myService, myService, myContract);
			var c2 = Broker.BeginConversation(myService, myService, myContract);

			Broker.Send(new[] { c1, c2 }, myMessage, Encoding.UTF8.GetBytes("Hello World!"));

			ServiceBrokerMessageHandler checkHelloWorld = (c, t, b) => {
				Check.That(
					() => t.Name == "MyMessage",
					() => new StreamReader(b).ReadToEnd() == "Hello World!");
			};
			Check.That(
				() => myQ.TryReceive(Broker.GetTargetConversation(c1), checkHelloWorld, TimeSpan.Zero),
				() => myQ.TryReceive(Broker.GetTargetConversation(c2), checkHelloWorld, TimeSpan.Zero)
			);
		}

		public void can_create_conversation_channel() {
			var myQ = Broker.CreateQueue("MyQueue");
			var myMessage = Broker.CreateMessageType("MyMessage");
			var myContract = Broker.CreateContract("MyContract", myMessage);
			var myService = myQ.CreateService("MyQueue", myContract);

			var c1 = Broker.BeginConversation(myService, myService, myContract);

			var channel = Broker.OpenConversationChannel(c1);
			Check.That(() => channel is IChannel);
		}

		public void send_and_receive_on_conversation_channel() {
			var myQ = Broker.CreateQueue("MyQueue");
			var myMessage = Broker.CreateMessageType("System.String");
			var myContract = Broker.CreateContract("MyContract2", new [] { myMessage }, new[] { myMessage });
			var myService = myQ.CreateService("MyQueue", myContract);

			var c1 = Broker.BeginConversation(myService, myService, myContract);

			var channel = Broker.OpenConversationChannel(c1);
			channel.Send("Hello World");

			var json = new JsonMessageSerializer();
			ServiceBrokerMessageHandler getHelloSendGoodbye = (c, t, b) => {
				Check.That(() => json.Deserialize(b, t) == "Hello World");
				Broker.OpenConversationChannel(c).Send("Goodbye World!");
			};
			 
			Check.That(() => myQ.TryReceive(getHelloSendGoodbye, TimeSpan.Zero));

			Action<string,object> checkGoodbye = (t, b) => Check.That(() => (string)b == "Goodbye World!"); 
			Check.That(() => channel.TryReceive(checkGoodbye, TimeSpan.Zero));
		}

		public void request_reply() {
			var initiatorQueue = Broker.CreateQueue("Initiator");
			var targetQueue = Broker.CreateQueue("Target");
			var requestMessage = Broker.CreateMessageType("Request");
			var replyMessage = Broker.CreateMessageType("Reply");
			var contract = Broker.CreateContract("RequestReply", 
				sentByInitiator: new [] { requestMessage }, 
				sentByTarget:  new[] { replyMessage });

			var initiatorService = initiatorQueue.CreateService("Initiator", contract);
			var targetService = targetQueue.CreateService("Target", contract);

			var request = Broker.BeginConversation(initiatorService, targetService, contract);

			request.Send(requestMessage, Stream.Null);
			var requestReceived = targetQueue.TryReceive((c, t, b) => {
				Check.That(() => t.Name == "Request");
				c.Send(replyMessage, Stream.Null);
			}, TimeSpan.Zero);

			var replyReceived = initiatorQueue.TryReceive((c, t, b) => {
				Check.That(() => t.Name == "Reply");
			}, TimeSpan.Zero);
			Check.That(
				() => requestReceived,
				() => replyReceived);
		}

		public void shared_request_reply_type() {
			var initiatorQueue = Broker.CreateQueue("Initiator");
			var targetQueue = Broker.CreateQueue("Target");
			var requestReplyMessage = Broker.CreateMessageType("RequestReply");
			var contract = Broker.CreateContract("RequestReplySame", 
				sentByInitiator: new [] { requestReplyMessage }, 
				sentByTarget: new[] { requestReplyMessage });
		}

		public void contracts_must_have_at_least_one_message_type() {
			Check.Exception<InvalidOperationException>(
				() => Broker.CreateContract("TheContract"));
		}

		public void delete_queue_removes_all_assoicated_services() {
			var theQueue = Broker.CreateQueue("TheQueue");
			var conttract = Broker.CreateContract("TheContract", ServiceBrokerMessageType.Default);

			var s1 = theQueue.CreateService("Service1", conttract);
			var s2 = theQueue.CreateService("Service2", conttract);

			Broker.DeleteQueue(theQueue.Name);

			Check.That(
				() => !Db.ObjectExists(s1.Name),
				() => !Db.ObjectExists(s2.Name));
		}
	}
}
