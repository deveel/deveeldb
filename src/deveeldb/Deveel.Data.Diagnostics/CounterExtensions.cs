using System;
using System.Globalization;

using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Diagnostics {
	public static class CounterExtensions {
		public static T ValueAs<T>(this ICounter counter) {
			if (counter.Value == null)
				return default(T);

			if (counter.Value is T)
				return (T) counter.Value;

			return (T) Convert.ChangeType(counter.Value, typeof(T), CultureInfo.InvariantCulture);
		}

		public static SqlNumber ValueAsNumber(this ICounter counter) {
			if (counter.Value == null)
				return SqlNumber.Null;

			if (counter.Value is byte ||
				counter.Value is int)
				return new SqlNumber((int)counter.Value);
			if (counter.Value is long)
				return new SqlNumber((long)counter.Value);
			if (counter.Value is float ||
				counter.Value is double)
				return new SqlNumber((double)counter.Value);

			throw new InvalidOperationException();
		}
	}
}
