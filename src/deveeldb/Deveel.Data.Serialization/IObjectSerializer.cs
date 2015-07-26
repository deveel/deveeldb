using System;
using System.IO;

namespace Deveel.Data.Serialization {
	public interface IObjectSerializer {
		void Serialize(object obj, Stream outputStream);

		object Deserialize(Stream inputStream);
	}
}
