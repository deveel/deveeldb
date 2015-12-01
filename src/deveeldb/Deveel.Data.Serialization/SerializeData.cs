// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Collections.Generic;

namespace Deveel.Data.Serialization {
	public sealed class SerializeData {
		private readonly Dictionary<string, KeyValuePair<Type, object>> values;

		internal SerializeData(Type graphType) {
			GraphType = graphType;
			values = new Dictionary<string, KeyValuePair<Type, object>>();
		}

		public Type GraphType { get; private set; }

		internal IEnumerable<KeyValuePair<string, KeyValuePair<Type, object>>> Values {
			get { return values; }
		} 

		public void SetValue(string key, Type type, object value) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");
			if (type == null)
				throw new ArgumentNullException("type");

			if (type.IsArray) {
				if (!IsSupported(type.GetElementType()))
				throw new NotSupportedException(String.Format("The element type '{0}' of the array is not supported.",
					type.GetElementType()));
			} else if (!type.IsArray && !IsSupported(type))
				throw new NotSupportedException(String.Format("The type '{0}' is not supported.", type));

			if (value != null && !type.IsInstanceOfType(value))
				throw new ArgumentException(
					String.Format("The specified object value is not assignable from the type '{0}' specified.", type));

			values[key] = new KeyValuePair<Type, object>(type, value);
		}

		private static bool IsSupported(Type type) {
			return type.IsPrimitive ||
			       type == typeof (string) ||
			       Attribute.IsDefined(type, typeof(SerializableAttribute));
		}

		public void SetValue(string key, object value) {
			if (value == null)
				return;

			var type = value.GetType();
			SetValue(key, type, value);
		}

		public void SetValue(string key, bool value) {
			SetValue(key, typeof(bool), value);
		}

		public void SetValue(string key, byte value) {
			SetValue(key, typeof(byte), value);
		}

		public void SetValue(string key, short value) {
			SetValue(key, typeof(short), value);
		}

		public void SetValue(string key, int value) {
			SetValue(key, typeof(int), value);
		}

		public void SetValue(string key, long value) {
			SetValue(key, typeof(long), value);
		}

		public void SetValue(string key, float value) {
			SetValue(key, typeof(float), value);
		}

		public void SetValue(string key, double value) {
			SetValue(key, typeof(double), value);
		}

		public void SetValue(string key, string value) {
			SetValue(key, typeof(string), value);
		}
	}
}
