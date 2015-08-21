using System;
using System.Data.SqlClient;
using System.Linq;
using Cone;

namespace Drunkcod.Data.ServiceBroker.Specs
{
	[Describe(typeof(SqlServerServiceBroker))]
	public class SqlServerServiceBrokerSpec
	{
		readonly string DbName = $"{typeof(SqlServerServiceBroker).FullName}.Spec";
		const string ConnectionString = "Server=.;Integrated Security=SSPI";
		SqlServerServiceBroker Broker;
		string BrokerConnectionString => ConnectionString + ";Database=" + DbName;

		[BeforeAll]
		public void create_empty_database() {
			using(var db = new SqlConnection(ConnectionString))
			using(var cmd = new SqlCommand($"create database [{DbName}]", db)) {
				db.Open();
				cmd.ExecuteNonQuery();
			}
		}

		[AfterAll]
		public void drop_test_database() {
			SqlConnection.ClearAllPools();
			using(var db = new SqlConnection(ConnectionString))
			using(var cmd = new SqlCommand($"drop database [{DbName}]", db)) {
				db.Open();
				cmd.ExecuteNonQuery();
			}
		}

		[BeforeEach]
		public void enable_broker() {
			Broker = new SqlServerServiceBroker(BrokerConnectionString);
			Broker.EnableBroker();
		}

		[AfterEach]
		public void drop_user_queues() {
			foreach(var item in Broker.GetQueues())
				Broker.DeleteQueue(item.Name);
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
	}
}
