using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace Drunkcod.Data.ServiceBroker
{
	public class SqlCommander
	{
		static readonly Action<SqlParameterCollection> NoSetup = _ => { };

		readonly string connectionString;

		public SqlCommander(string connectionString) {
			this.connectionString = connectionString;
		}

		public int ExecuteNonQuery(string query, Action<SqlParameterCollection> setup) =>
			Execute(query, setup, x => x.ExecuteNonQuery());

		public object ExecuteScalar(string query, Action<SqlParameterCollection> setup) =>
			Execute(query, setup, x => x.ExecuteScalar());

		T Execute<T>(string query, Action<SqlParameterCollection> setup, Func<SqlCommand, T> exec) {
			var cmd = NewCommand(query);
			try {
				setup(cmd.Parameters);
				cmd.Connection.Open();
				return exec(cmd);
			} finally {
				cmd.Connection.Dispose();
				cmd.Dispose();
			}
		}

		public void ExecuteNonQuery(string query) =>
			ExecuteNonQuery(query, NoSetup);

		public object ExecuteScalar(string query) =>
			ExecuteScalar(query, NoSetup);

		public IEnumerable<T> ExecuteReader<T>(string query, CommandBehavior commandBehavior, Func<IDataRecord,T> convert) =>
			ExecuteReader<T>(query, NoSetup, commandBehavior, convert);

		public IEnumerable<T> ExecuteReader<T>(string query, Action<SqlParameterCollection> setup, CommandBehavior commandBehavior, Func<IDataRecord,T> convert) {
			var cmd = NewCommand(query);
			setup(cmd.Parameters);
			cmd.Connection.Open();
			using(var reader = cmd.ExecuteReader(CommandBehavior.CloseConnection | commandBehavior)) {
				while(reader.Read())
					yield return convert(reader);
			}
		}

		public SqlCommand NewCommand(string query) =>
			new SqlCommand(query, new SqlConnection(connectionString));

		public bool ObjectExists(string name) =>
			!(ExecuteScalar("select object_id(@name)", x => x.AddWithValue("@name", name)) is DBNull);
	}
}