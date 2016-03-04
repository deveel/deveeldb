using System;

namespace System.Runtime.Serialization {
	public abstract class SerializationBinder {
		public abstract Type BindToType(string assemblyName, string typeName);

		public virtual void BindToName(Type type, out string assemblyName, out string typeName) {
			assemblyName = null;
			typeName = null;
		}
	}
}
