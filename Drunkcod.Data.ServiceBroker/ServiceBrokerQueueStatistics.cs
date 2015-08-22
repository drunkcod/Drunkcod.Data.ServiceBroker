using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Drunkcod.Data.ServiceBroker
{
	public class ServiceBrokerQueueStatistics : IEnumerable<QueueStatisticsRow>
	{
		readonly Dictionary<string,QueueStatisticsRow> rows = new Dictionary<string, QueueStatisticsRow>(); 

		public int MessageCount => rows.Sum(x => x.Value.Count);

		public QueueStatisticsRow this[string name] => rows[name];

		internal void Add(string name, QueueStatisticsRow row) {
			rows.Add(name, row);
		}

		IEnumerator<QueueStatisticsRow> IEnumerable<QueueStatisticsRow>.GetEnumerator() {
			return rows.Values.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() { return rows.Values.GetEnumerator(); }
	}
}