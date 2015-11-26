using System;
using System.Collections.Generic;

namespace Deveel.Data.Serialization {
	public sealed class SerializationGraph {
		private readonly Dictionary<string, KeyValuePair<Type, object>> values;

		internal SerializationGraph(Type graphType) {
			GraphType = graphType;
			values = new Dictionary<string, KeyValuePair<Type, object>>();
		}

		public Type GraphType { get; private set; }

		internal IEnumerable<KeyValuePair<string, KeyValuePair<Type, object>>> Values {
			get { return values; }
		} 

		public void Write(string key, Type type, object value) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");
			if (type == null)
				throw new ArgumentNullException("type");

			if (type.IsArray &&
			    !IsSupported(type.GetElementType()))
				throw new NotSupportedException(String.Format("The element type '{0}' of the array is not supported.",
					type.GetElementType()));
			if (!IsSupported(type))
				throw new NotSupportedException(String.Format("The type '{0}' is not supportd.", type));

			if (!type.IsInstanceOfType(value))
				throw new ArgumentException(
					String.Format("The specified object value is not assignable from the type '{0}' specified.", type));

			values[key] = new KeyValuePair<Type, object>(type, value);
		}

		private static bool IsSupported(Type type) {
			return !type.IsPrimitive &&
			       type != typeof (string) &&
			       !typeof (ISerializable).IsAssignableFrom(type);
		}

		public void Write(string key, object value) {
			if (value == null)
				throw new ArgumentNullException("value");

			var type = value.GetType();
			Write(key, type, value);
		}

		public void Write(string key, bool value) {
			Write(key, typeof(bool), value);
		}

		public void Write(string key, byte value) {
			Write(key, typeof(byte), value);
		}

		public void Write(string key, short value) {
			Write(key, typeof(short), value);
		}

		public void Write(string key, int value) {
			Write(key, typeof(int), value);
		}

		public void Write(string key, long value) {
			Write(key, typeof(long), value);
		}

		public void Write(string key, float value) {
			Write(key, typeof(float), value);
		}

		public void Write(string key, double value) {
			Write(key, typeof(double), value);
		}

		public void Write(string key, string value) {
			Write(key, typeof(string), value);
		}
	}
}
