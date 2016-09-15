namespace Drunkcod.Data.ServiceBroker
{
	public class ServiceBrokerQueueMessage
	{
		public ServiceBrokerQueueMessage(ServiceBrokerMessageType messageType, byte[] body) {
			this.MessageType = messageType;
			this.MessageBody = body;
		}

		public readonly ServiceBrokerMessageType MessageType;
		public byte[] MessageBody;
	}
}