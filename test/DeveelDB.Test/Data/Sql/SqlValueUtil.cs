using System;

namespace Deveel.Data.Sql {
	public static class SqlValueUtil {
		public static ISqlValue FromObject(object value) {
			if (value == null)
				return SqlNull.Value;

			if (value is bool)
				return (SqlBoolean)(bool)value;

			if (value is byte)
				return (SqlNumber)(byte)value;
			if (value is int)
				return (SqlNumber)(int)value;
			if (value is short)
				return (SqlNumber)(short)value;
			if (value is long)
				return (SqlNumber)(long)value;
			if (value is float)
				return (SqlNumber)(float)value;
			if (value is double)
				return (SqlNumber)(double)value;

			if (value is string)
				return new SqlString((string)value);

			if (value is ISqlValue)
				return (ISqlValue) value;

			throw new NotSupportedException();
		}
	}
}