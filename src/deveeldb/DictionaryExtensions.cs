// 
//  Copyright 2010-2017 Deveel
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
using System.Linq;
using System.Reflection;

namespace Deveel {
	public static class DictionaryExtensions {
		public static T GetValue<T>(this IEnumerable<KeyValuePair<string, object>> pairs, string key) {
			return GetValue<T>(pairs, key, CultureInfo.InvariantCulture);
		}

		public static T GetValue<T>(this IEnumerable<KeyValuePair<string, object>> pairs, string key, IFormatProvider provider) {
			IDictionary<string, object> dictionary = pairs as IDictionary<string, object>;
			if (dictionary == null)
				dictionary = pairs.ToDictionary(x => x.Key, y => y.Value);

			return dictionary.GetValue<T>(key, provider);
		}

		public static T GetValue<T>(this IDictionary<string, object> dictionary, string key) {
			return GetValue<T>(dictionary, key, CultureInfo.InvariantCulture);
		}

		public static T GetValue<T>(this IDictionary<string, object> dictionary, string key, IFormatProvider provider) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException(nameof(key));

			if (dictionary == null)
				return default(T);

			object value;
			if (!dictionary.TryGetValue(key, out value) ||
				value == null)
				return default(T);

			if (!(value is T)) {
				if (value is string &&
				    typeof(T).GetTypeInfo().IsEnum)
					return (T) Enum.Parse(typeof(T), (string) value, true);
				if (provider == null)
					throw new ArgumentNullException(nameof(provider));

				var nullableType = Nullable.GetUnderlyingType(typeof(T));
				if (nullableType != null) {
					value = Convert.ChangeType(value, nullableType, provider);
				} else {
					value = Convert.ChangeType(value, typeof(T), provider);
				}
			}

			return (T) value;
		}
	}
}
