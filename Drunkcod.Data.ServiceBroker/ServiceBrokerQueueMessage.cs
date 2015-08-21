namespace Drunkcod.Data.ServiceBroker
{
	public class ServiceBrokerQueueMessage
	{
		public ServiceBrokerQueueMessage(ServiceBrokerMessageType messageType) {
			this.MessageType = messageType;
		}

		public readonly ServiceBrokerMessageType MessageType;
	}
}