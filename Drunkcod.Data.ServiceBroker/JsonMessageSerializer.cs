using System;
using System.IO;
using System.Text;
using Newtonsoft.Json;

namespace Drunkcod.Data.ServiceBroker
{
	public interface IMessageSerializer
	{
		Stream Serialize(object item);
		object Deserialize(Stream body, ServiceBrokerMessageType  messageType);
		T Deserialize<T>(Stream body);
	}

	public sealed class JsonMessageSerializer : IMessageSerializer
	{
		static readonly Encoding Utf8NoBom = new UTF8Encoding(false);
		static readonly JsonSerializer Serializer = new JsonSerializer();

		public Stream Serialize(object item) {
			var body = new MemoryStream();
			using(var writer = new StreamWriter(body, Utf8NoBom, 512, true))
				Serializer.Serialize(writer, item);
			body.Position = 0;
			return body;
		}

		public object Deserialize(Stream body, ServiceBrokerMessageType messageType) {
			return Deserialize(body, GetType(messageType));
		}

		public T Deserialize<T>(Stream body) {
			return (T)Deserialize(body, typeof(T));
		}

		object Deserialize(Stream body, Type type) {
			using(var reader = new StreamReader(body, Utf8NoBom))
			using(var json = new JsonTextReader(reader))
				return Serializer.Deserialize(json, type);
		}



		static Type GetType(ServiceBrokerMessageType messageType)
		{
			var type = Type.GetType(messageType.Name);
			if(type == null)
				throw new InvalidOperationException("Unable to locate messageType: " + messageType.Name);
			return type;
		}
	}
}