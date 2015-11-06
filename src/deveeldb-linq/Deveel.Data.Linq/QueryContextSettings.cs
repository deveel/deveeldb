using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;

namespace Deveel.Data.Linq {
	public sealed class QueryContextSettings : IEnumerable<KeyValuePair<String, object>> {
		private readonly Dictionary<string, object> values;

		public QueryContextSettings() {
			values = new Dictionary<string, object>();
		}

		public object this[string key] {
			get { return GetValue(key); }
			set { SetValue(key, value); }
		}

		public string UserName {
			get { return GetValue<string>("UserName"); }
			set { SetValue("UserName", value); }
		}

		public string Password {
			get { return GetValue<string>("Password"); }
			set { SetValue("Password", value); }
		}

		public object GetValue(string key) {
			object value;
			if (!values.TryGetValue(key, out value))
				return null;

			return value;
		}

		public T GetValue<T>(string key) {
			var value = GetValue(key);

			if (value == null)
				return default(T);

			if (!typeof (T).IsInstanceOfType(value))
				value = Convert.ChangeType(value, typeof (T), CultureInfo.InvariantCulture);

			return (T) value;
		}

		public void SetValue(string key, object value) {
			values[key] = value;
		}

		public static QueryContextSettings Parse(string s) {
			throw new NotImplementedException();
		}

		public IEnumerator<KeyValuePair<string, object>> GetEnumerator() {
			return values.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}
	}
}
