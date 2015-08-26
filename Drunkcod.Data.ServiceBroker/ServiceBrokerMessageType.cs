using System.ComponentModel;

namespace Drunkcod.Data.ServiceBroker
{
	public struct ServiceBrokerMessageType
	{
		public readonly string Name;
		internal ServiceBrokerMessageType(string name) { this.Name = name; }

		public static readonly ServiceBrokerMessageType Default = new ServiceBrokerMessageType("DEFAULT");
	}
}