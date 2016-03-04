using System;
using System.IO;

namespace System.Runtime.Serialization {
	public interface IFormatter {
		SerializationBinder Binder { get; set; }

		StreamingContext Context { get; set; }

		ISurrogateSelector SurrogateSelector { get; set; }


		object Deserialize(Stream serializationStream);

		void Serialize(Stream serializationStream, object graph);
	}
}
