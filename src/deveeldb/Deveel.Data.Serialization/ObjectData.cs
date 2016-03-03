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
using System.Globalization;

namespace Deveel.Data.Serialization {
	public sealed class ObjectData {
		private readonly Dictionary<string, object> values;
		 
		internal ObjectData(Type entityType, IEnumerable<KeyValuePair<string, object>> values) {
			EntityType = entityType;
			this.values = new Dictionary<string, object>();
			if (values != null) {
				foreach (var pair in values) {
					this.values[pair.Key] = pair.Value;
				}
			}
		}

		public Type EntityType { get; private set; }

		public int Count {
			get { return values.Count; }
		}

		public bool HasValue(string key) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			return values.ContainsKey(key);
		}

		public object GetValue(string key) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			object value;
			if (!values.TryGetValue(key, out value))
				return null;

			return value;
		}

		public T GetValue<T>(string key) {
			var value = GetValue(key);
			if (value == null)
				return default(T);

			if (value is T)
				return (T) value;

			if (!(value is IConvertible))
				throw new InvalidCastException();

			return (T) Convert.ChangeType(value, typeof (T), CultureInfo.InvariantCulture);
		}

		public bool GetBoolean(string key) {
			return GetValue<bool>(key);
		}

		public byte GetByte(string key) {
			return GetValue<byte>(key);
		}

		public short GetInt16(string key) {
			return GetValue<short>(key);
		}

		public int GetInt32(string key) {
			return GetValue<int>(key);
		}

		public long GetInt64(string key) {
			return GetValue<long>(key);
		}

		public float GetSingle(string key) {
			return GetValue<float>(key);
		}

		public double GetDouble(string key) {
			return GetValue<double>(key);
		}

		public string GetString(string key) {
			return GetValue<string>(key);
		}
	}
}