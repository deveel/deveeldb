using System;
using System.IO;

namespace Deveel.Data.Serialization {
	public interface IObjectBinarySerializer : IObjectSerializer {
		void Serialize(object obj, BinaryWriter writer);

		object Deserialize(BinaryReader reader);
	}
}
