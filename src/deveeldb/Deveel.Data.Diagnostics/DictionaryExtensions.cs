using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Diagnostics {
	static class DictionaryExtensions {
		public static T GetValue<T>(this IEnumerable<KeyValuePair<string, object>> pairs, string key,
			IFormatProvider formatProvider) {
			return pairs.ToDictionary(x => x.Key, y => y.Value).GetValue<T>(key, formatProvider);
		}

		public static T GetValue<T>(this IDictionary<string, object> dictionary, string key, IFormatProvider formatProvider) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			if (dictionary == null)
				return default(T);

			object value;
			if (!dictionary.TryGetValue(key, out value) ||
				value == null)
				return default(T);

			if (!(value is T)) {
#if !PCL
				if (value is string &&
				    typeof(T).IsEnum)
					return (T) Enum.Parse(typeof(T), (string) value, true);
#endif
				if (formatProvider == null)
					throw new ArgumentNullException("formatProvider");

				var nullableType = Nullable.GetUnderlyingType(typeof(T));
				if (nullableType != null) {
					value = Convert.ChangeType(value, nullableType, formatProvider);
				} else {
					value = Convert.ChangeType(value, typeof(T), formatProvider);
				}
			}

			return (T) value;
		}
	}
}
