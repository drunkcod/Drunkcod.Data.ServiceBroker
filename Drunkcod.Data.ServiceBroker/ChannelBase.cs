using System;
using System.IO;
using System.Text;
using Newtonsoft.Json;

namespace Drunkcod.Data.ServiceBroker
{
	class ChannelBase
	{
		static readonly Encoding Utf8NoBom = new UTF8Encoding(false);
		static readonly JsonSerializer serializer = new JsonSerializer();

		protected Stream Serialize(object item) {
			var body = new MemoryStream();
			using(var writer = new StreamWriter(body, Utf8NoBom, 512, true))
				serializer.Serialize(writer, item);
			body.Position = 0;
			return body;
		}

		protected object Deserialize(Stream body, Type type) {
			using(var reader = new StreamReader(body, Utf8NoBom))
			using(var json = new JsonTextReader(reader))
				return serializer.Deserialize(json, type);
		}
	}
}