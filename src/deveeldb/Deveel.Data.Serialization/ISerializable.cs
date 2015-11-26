using System;

namespace Deveel.Data.Serialization {
	public interface ISerializable {
		void WriteToGraph(SerializationGraph graph);
	}
}
