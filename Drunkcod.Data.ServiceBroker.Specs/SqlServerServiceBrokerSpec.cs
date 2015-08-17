using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cone;

namespace Drunkcod.Data.ServiceBroker.Specs
{
	[Describe(typeof(SqlServerServiceBroker))]
	public class SqlServerServiceBrokerSpec
	{
		readonly string DbName = $"{typeof(SqlServerServiceBroker).FullName}.Spec";
		const string ConnectionString = "Server=.;Integrated Security=SSPI";
		SqlServerServiceBroker Broker;

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
			using(var db = new SqlConnection(ConnectionString))
			using(var cmd = new SqlCommand($"alter database [{DbName}] set single_user with rollback immediate; drop database [{DbName}]", db)) {
				db.Open();
				cmd.ExecuteNonQuery();
			}
		}

		[BeforeEach]
		public void enable_broker() {
			Broker = new SqlServerServiceBroker(ConnectionString + ";Database=" + DbName);
			Broker.EnableBroker();
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
	}
}
