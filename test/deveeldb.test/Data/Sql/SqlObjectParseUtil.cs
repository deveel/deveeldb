// 
//  Copyright 2010-2018 Deveel
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