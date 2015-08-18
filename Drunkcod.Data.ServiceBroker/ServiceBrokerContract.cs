namespace Drunkcod.Data.ServiceBroker
{
	public struct ServiceBrokerContract
	{
		public readonly string Name;
		internal ServiceBrokerContract(string name) { this.Name = name; }
	}
}