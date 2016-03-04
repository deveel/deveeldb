using System;

namespace System.Runtime.Serialization {
	public struct SerializationEntry {
		internal SerializationEntry(string name, Type objectType, object value) {
			Name = name;
			ObjectType = objectType;
			Value = value;
		}

		public string Name { get; private set; }

		public object Value { get; private set; }

		public Type ObjectType { get; private set; }
	}
}
