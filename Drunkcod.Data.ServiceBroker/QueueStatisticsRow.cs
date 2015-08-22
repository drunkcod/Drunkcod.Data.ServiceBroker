namespace Drunkcod.Data.ServiceBroker
{
	public struct QueueStatisticsRow
	{
		public readonly ServiceBrokerMessageType MessageType;
		public readonly int Count;

		public QueueStatisticsRow(ServiceBrokerMessageType messageType, int count) {
			this.MessageType = messageType;
			this.Count = count;
		}
	}
}