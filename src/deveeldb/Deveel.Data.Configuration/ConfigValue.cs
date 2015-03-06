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
using System.Data;
using System.Globalization;

namespace Deveel.Data.Configuration {
	public sealed class ConfigValue {
		public ConfigValue(ConfigKey key, object value) {
			if (key == null) 
				throw new ArgumentNullException("key");

			if (value != null &&
				!key.ValueType.IsInstanceOfType(value))
				throw new ArgumentException();

			Key = key;
			Value = value;
		}


		public ConfigKey Key { get; private set; }

		public object Value { get; private set; }

		public T ToType<T>() {
			if (Value == null)
				return default(T);

			if (Value is T)
				return (T) Value;

			if (!(Value is IConvertible))
				throw new InvalidCastException();

			return (T) Convert.ChangeType(Value, typeof (T), CultureInfo.InvariantCulture);
		}
	}
}