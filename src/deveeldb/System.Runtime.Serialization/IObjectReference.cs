using System;

namespace System.Runtime.Serialization {
	public interface IObjectReference {
		object GetRealObject(StreamingContext context);
	}
}
