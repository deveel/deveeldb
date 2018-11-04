using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Deveel.Data.Serialization {
	static class BinarySerializeUtil {
		public static T Serialize<T>(T obj) {
			var serializer = new BinaryFormatter();
			var stream = new MemoryStream();

			serializer.Serialize(stream, obj);

			stream.Seek(0, SeekOrigin.Begin);

			return (T) serializer.Deserialize(stream);
		}
	}
}