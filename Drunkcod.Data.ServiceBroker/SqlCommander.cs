using System;
using System.Data.SqlClient;

namespace Drunkcod.Data.ServiceBroker
{
	public class SqlCommander
	{
		readonly string connectionString;

		public SqlCommander(string connectionString) {
			this.connectionString = connectionString;
		}

		public void ExecuteNonQuery(string query, Action<SqlParameterCollection> setup) {
			var cmd = NewCommand(query);
			try {
				setup(cmd.Parameters);
				cmd.Connection.Open();
				cmd.ExecuteNonQuery();
			} finally {
				cmd.Connection.Dispose();
				cmd.Dispose();
			}
		}

		public object ExecuteScalar(string query) {
			var cmd = NewCommand(query);
			try {
				cmd.Connection.Open();
				return cmd.ExecuteScalar();
			} finally {
				cmd.Connection.Dispose();
				cmd.Dispose();
			}
		}

		public SqlCommand NewCommand(string query) {
			return new SqlCommand(query, new SqlConnection(connectionString));
		}
	}
}