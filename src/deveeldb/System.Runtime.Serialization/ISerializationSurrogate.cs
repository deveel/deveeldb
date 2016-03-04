using System;

namespace System.Runtime.Serialization {
	public interface ISerializationSurrogate {
		void GetObjectData(object obj, SerializationInfo info, StreamingContext context);

		object SetObjectData(object obj, SerializationInfo info, StreamingContext context, ISurrogateSelector selector);
	}
}
