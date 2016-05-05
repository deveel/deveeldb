using System;
using System.Globalization;

using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Diagnostics {
	public sealed class Counter {
		internal Counter(string name, object value) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			Name = name;
			Value = value;
		}

		public string Name { get; private set; }

		public object Value { get; private set; }

		internal void Increment() {
			object value = Value;
			if (value == null) {
				value = 1L;
			} else {
				if (value is long) {
					value = ((long)value) + 1;
				} else if (value is int) {
					value = (int)value + 1;
				} else if (value is double) {
					value = (double)value + 1;
				} else {
					throw new InvalidOperationException(String.Format("The value for '{0}' is not a numeric.", Name));
				}
			}

			Value = value;
		}

		public T ValueAs<T>() {
			if (Value == null)
				return default(T);

			if (Value is T)
				return (T) Value;

			return (T) Convert.ChangeType(Value, typeof(T), CultureInfo.InvariantCulture);
		}

		public SqlNumber ValueAsNumber() {
			if (Value == null)
				return SqlNumber.Null;

			if (Value is byte ||
				Value is int)
				return new SqlNumber((int)Value);
			if (Value is long)
				return new SqlNumber((long)Value);
			if (Value is float ||
				Value is double)
				return new SqlNumber((double)Value);

			throw new InvalidOperationException();
		}
	}
}
