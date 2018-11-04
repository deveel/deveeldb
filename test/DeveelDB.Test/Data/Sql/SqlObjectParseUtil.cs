using System;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql {
	static class SqlObjectParseUtil {
		private static bool TryParseValue(SqlTypeCode typeCode, string s, IFormatProvider provider, out ISqlValue outValue, out Exception error) {
			try {
				switch (typeCode) {
					case SqlTypeCode.TinyInt:
					case SqlTypeCode.SmallInt:
					case SqlTypeCode.Integer:
					case SqlTypeCode.BigInt:
					case SqlTypeCode.Real:
					case SqlTypeCode.Double:
					case SqlTypeCode.Decimal:
					case SqlTypeCode.Float:
					case SqlTypeCode.Numeric:
					case SqlTypeCode.VarNumeric: {
						error = null;
						var result = SqlNumber.TryParse(s, provider, out var value);
						outValue = value;

						return result;
					}
					case SqlTypeCode.Bit:
					case SqlTypeCode.Boolean: {
						error = null;
						var result = SqlBoolean.TryParse(s, out var value);
						outValue = value;

						return result;
					}
					case SqlTypeCode.Date:
					case SqlTypeCode.DateTime:
					case SqlTypeCode.TimeStamp: {
						error = null;
						var result = SqlDateTime.TryParse(s, out var value);
						outValue = value;

						return result;
					}
					case SqlTypeCode.YearToMonth: {
						error = null;
						var result = SqlYearToMonth.TryParse(s, out var value);
						outValue = value;

						return result;
					}
					case SqlTypeCode.DayToSecond: {
						error = null;
						var result = SqlDayToSecond.TryParse(s, out var value);
						outValue = value;

						return result;
					}
					default: {
						error = new FormatException($"The type {typeCode} does not support parsing");
						outValue = null;

						return false;
					}
				}
			} catch (Exception ex) {
				error = ex;
				outValue = null;

				return false;
			}
		}

		public static SqlObject Parse(SqlType type, string s, IFormatProvider provider) {
			if (!TryParseValue(type.TypeCode, s, provider, out var value, out var error))
				throw error;

			return new SqlObject(type, value);
		}
	}
}