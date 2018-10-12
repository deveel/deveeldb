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
using System.IO;

namespace Deveel.Data.Sql.Types {
	public sealed class SqlDateTimeType : SqlType {
		public SqlDateTimeType(SqlTypeCode sqlType)
			: base(sqlType) {
			AssertDateType(sqlType);
		}

		private static void AssertDateType(SqlTypeCode sqlType) {
			if (!IsDateType(sqlType))
				throw new ArgumentException($"The SQL type {sqlType} is not a valid DATETIME", nameof(sqlType));
		}

		internal static bool IsDateType(SqlTypeCode sqlType) {
			return sqlType == SqlTypeCode.Date ||
			       sqlType == SqlTypeCode.Time ||
			       sqlType == SqlTypeCode.TimeStamp ||
			       sqlType == SqlTypeCode.DateTime;
		}

		public override bool IsComparable(SqlType type) {
			return type is SqlDateTimeType;
		}

		public override ISqlValue Add(ISqlValue a, ISqlValue b) {
			if (!(a is SqlDateTime))
				throw new ArgumentException();

			var date = (SqlDateTime) a;
			if (b is SqlYearToMonth) {
				var ytm = (SqlYearToMonth) b;
				return date.Add(ytm);
			}

			if (b is SqlDayToSecond) {
				var dts = (SqlDayToSecond) b;
				return date.Add(dts);
			}

			return base.Add(a, b);
		}

		public override ISqlValue Subtract(ISqlValue a, ISqlValue b) {
			if (!(a is SqlDateTime))
				throw new ArgumentException();

			var date = (SqlDateTime)a;
			if (b is SqlYearToMonth) {
				var ytm = (SqlYearToMonth)b;
				return date.Subtract(ytm);
			}

			if (b is SqlDayToSecond) {
				var dts = (SqlDayToSecond)b;
				return date.Subtract(dts);
			}

			return base.Subtract(a, b);
		}

		public override SqlBoolean Equal(ISqlValue a, ISqlValue b) {
			if (a is SqlDateTime && b is SqlDateTime) {
				var x = (SqlDateTime) a;
				var y = (SqlDateTime) b;

				return x == y;
			}

			return base.Equal(a, b);
		}

		public override SqlBoolean NotEqual(ISqlValue a, ISqlValue b) {
			if (a is SqlDateTime && b is SqlDateTime) {
				var x = (SqlDateTime)a;
				var y = (SqlDateTime)b;

				return x != y;
			}

			return base.NotEqual(a, b);
		}

		public override SqlBoolean Greater(ISqlValue a, ISqlValue b) {
			if (a is SqlDateTime && b is SqlDateTime) {
				var x = (SqlDateTime)a;
				var y = (SqlDateTime)b;

				return x > y;
			}

			return base.Greater(a, b);
		}

		public override SqlBoolean GreaterOrEqual(ISqlValue a, ISqlValue b) {
			if (a is SqlDateTime && b is SqlDateTime) {
				var x = (SqlDateTime)a;
				var y = (SqlDateTime)b;

				return x >= y;
			}

			return base.GreaterOrEqual(a, b);
		}

		public override SqlBoolean Less(ISqlValue a, ISqlValue b) {
			if (a is SqlDateTime && b is SqlDateTime) {
				var x = (SqlDateTime)a;
				var y = (SqlDateTime)b;

				return x < y;
			}

			return base.Less(a, b);
		}

		public override SqlBoolean LessOrEqual(ISqlValue a, ISqlValue b) {
			if (a is SqlDateTime && b is SqlDateTime) {
				var x = (SqlDateTime)a;
				var y = (SqlDateTime)b;

				return x <= y;
			}

			return base.LessOrEqual(a, b);
		}

		public override bool CanCastTo(ISqlValue value, SqlType destType) {
			return destType is SqlCharacterType ||
			       destType is SqlDateTimeType ||
			       destType is SqlNumericType;
		}

		public override ISqlValue Cast(ISqlValue value, SqlType destType) {
			if (!(value is SqlDateTime))
				throw new ArgumentException("DATETIME type cannot cast only from a SQL DATETIME");

			var date = (SqlDateTime) value;

			if (destType is SqlCharacterType)
				return ToString(date, (SqlCharacterType) destType);
			if (destType is SqlNumericType)
				return ToNumber(date);
			if (destType is SqlDateTimeType)
				return ToDateTime(date, (SqlDateTimeType) destType);

			return base.Cast(value, destType);
		}

		private ISqlValue ToDateTime(SqlDateTime date, SqlDateTimeType destType) {
			return destType.NormalizeValue(date);
		}

		private SqlNumber ToNumber(SqlDateTime date) {
			return (SqlNumber) date.Ticks;
		}

		private ISqlString ToString(SqlDateTime date, SqlCharacterType destType) {
			var dateString = ToSqlString(date);
			var s = new SqlString(dateString);

			return (ISqlString) destType.NormalizeValue(s);
		}

		public override ISqlValue NormalizeValue(ISqlValue value) {
			if (value is SqlNull)
				return value;

			if (!(value is SqlDateTime))
				throw new ArgumentException();

			var date = (SqlDateTime) value;

			switch (TypeCode) {
				case SqlTypeCode.Time:
					return date.TimePart;
				case SqlTypeCode.Date:
					return date.DatePart;
			}

			return base.NormalizeValue(value);
		}

		public override bool IsInstanceOf(ISqlValue value) {
			return value is SqlDateTime || value is SqlNull;
		}

		public override string ToSqlString(ISqlValue obj) {
			if (!(obj is SqlDateTime))
				throw new ArgumentException();

			var date = (SqlDateTime) obj;
			switch (TypeCode) {
				case SqlTypeCode.Time:
					return date.ToTimeString().Value;
				case SqlTypeCode.Date:
					return date.ToDateString().Value;
				default:
					return date.ToString();
			}
		}
	}
}