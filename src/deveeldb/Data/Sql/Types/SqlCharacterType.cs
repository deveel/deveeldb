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
using System.Globalization;
using System.IO;

namespace Deveel.Data.Sql.Types {
	public sealed class SqlCharacterType : SqlType {
		public const int DefaultMaxSize = Int16.MaxValue;

		public SqlCharacterType(SqlTypeCode typeCode, int maxSize, CultureInfo locale) 
			: base(typeCode) {
			AssertIsString(typeCode);

			if (typeCode == SqlTypeCode.Char) {
				if (maxSize < 0)
					throw new ArgumentException("CHAR type requires a max length is specified");
			}

			MaxSize = maxSize;
			Locale = locale;
		}

		/// <summary>
		/// Gets the maximum number of characters that strings
		/// handled by this type can handle.
		/// </summary>
		public int MaxSize { get; }

		public bool HasMaxSize => MaxSize > 0;

		/// <summary>
		/// Gets the locale used to compare string values.
		/// </summary>
		/// <remarks>
		/// When this value is not specified, the schema or database locale
		/// is used to compare string values.
		/// </remarks>
		public CultureInfo Locale { get; }

		private static void AssertIsString(SqlTypeCode sqlType) {
			if (!IsStringType(sqlType))
				throw new ArgumentException(String.Format("The type {0} is not a valid STRING type.", sqlType), "sqlType");
		}

		internal static bool IsStringType(SqlTypeCode typeCode) {
			return typeCode == SqlTypeCode.String ||
			       typeCode == SqlTypeCode.VarChar ||
			       typeCode == SqlTypeCode.Char ||
			       typeCode == SqlTypeCode.LongVarChar ||
			       typeCode == SqlTypeCode.Clob;
		}

		public override bool IsInstanceOf(ISqlValue value) {
			return value is ISqlString || value is SqlNull;
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			if (TypeCode == SqlTypeCode.LongVarChar) {
				builder.Append("LONG CHARACTER VARYING");
			} else {
				base.AppendTo(builder);
			}

			if (MaxSize >= 0) {
				if (MaxSize == DefaultMaxSize)
					builder.Append("(MAX)");
				else
					builder.AppendFormat("({0})", MaxSize);
			}

			if (Locale != null) {
				builder.AppendFormat(" COLLATE '{0}'", Locale.Name);
			}
		}

		public override bool IsComparable(SqlType type) {
			// Are we comparing with another string type?
			if (type is SqlCharacterType) {
				var stringType = (SqlCharacterType)type;
				// If either locale is null return true
				if (Locale == null || stringType.Locale == null)
					return true;

				//TODO: Check batter on the locale comparison: we could compare
				//      neutral cultures

				// If the locales are the same return true
				return Locale.Equals(stringType.Locale);
			}

			// Only string types can be comparable
			return false;
		}

		public override int Compare(ISqlValue x, ISqlValue y) {
			if (x == null)
				throw new ArgumentNullException(nameof(x));

			if (!(x is ISqlString) ||
			    !(y is ISqlString))
				throw new ArgumentException("Cannot compare objects that are not strings.");

			return SqlStringCompare.Compare(Locale, (ISqlString) x, (ISqlString) y);
		}

		public override bool CanCastTo(ISqlValue value, SqlType destType) {
			return destType is SqlCharacterType ||
			       destType is SqlBinaryType ||
			       destType is SqlBooleanType ||
				   destType is SqlNumericType ||
				   destType is SqlDateTimeType ||
				   destType is SqlYearToMonthType ||
				   destType is SqlDayToSecondType;
		}

		public override ISqlValue NormalizeValue(ISqlValue value) {
			if (value is SqlNull)
				return value;

			if (!(value is ISqlString))
				throw new ArgumentException("Cannot normalize a value that is not a SQL string");

			if (value is SqlString) {
				var s = (SqlString) value;

				switch (TypeCode) {
					case SqlTypeCode.VarChar:
					case SqlTypeCode.String: {
						if (HasMaxSize && s.Length > MaxSize)
							throw new InvalidCastException(); // TODO: maybe a substring here?

						return s;
					}
					case SqlTypeCode.Char: {
						if (s.Length > MaxSize) {
							s = s.Substring(0, MaxSize);
						} else if (s.Length < MaxSize) {
							s = s.PadRight(MaxSize);
						}

						return s;
					}
				}
			}

			return base.NormalizeValue(value);
		}

		public override ISqlValue Cast(ISqlValue value, SqlType destType) {
			if (!(value is ISqlString))
				throw new ArgumentException("Cannot cast a non-string value using a string type");

			if (value is SqlString) {
				if (destType is SqlBooleanType)
					return ToBoolean((SqlString) value);
				if (destType is SqlNumericType)
					return ToNumber((SqlString) value, destType);
				if (destType is SqlCharacterType)
					return ToString((SqlString) value, destType);
				if (destType is SqlDateTimeType)
					return ToDateTime((SqlString) value, destType);
				if (destType is SqlYearToMonthType)
					return ToYearToMonth((SqlString) value);
				if (destType is SqlDayToSecondType)
					return ToDayToSecond((SqlString) value);
			}

			return base.Cast(value, destType);
		}

		private ISqlValue ToString(SqlString value, SqlType destType) {
			return destType.NormalizeValue(value);
		}

		private ISqlValue ToNumber(SqlString value, SqlType destType) {
				var locale = Locale ?? CultureInfo.InvariantCulture;
				SqlNumber number;
				if (!SqlNumber.TryParse(value.Value, locale, out number))
					throw new InvalidCastException();

			return destType.NormalizeValue(number);
		}


		private SqlBoolean ToBoolean(SqlString value) {
			if (value == null)
				throw new InvalidCastException();

			if (value.Equals(SqlBoolean.TrueString, true))
				return SqlBoolean.True;
			if (value.Equals(SqlBoolean.FalseString, true))
				return SqlBoolean.False;

			throw new InvalidCastException();
		}

		private ISqlValue ToDateTime(SqlString value, SqlType destType) {
			if (value == null)
				throw new InvalidCastException();

			SqlDateTime date;
			if (!SqlDateTime.TryParse(value.Value, out date))
				throw new InvalidCastException();

			return destType.NormalizeValue(date);
		}

		private ISqlValue ToDayToSecond(SqlString value) {
			SqlDayToSecond dts;
			if (!SqlDayToSecond.TryParse(value.Value, out dts))
				return SqlNull.Value;

			return dts;
		}

		private ISqlValue ToYearToMonth(SqlString value) {
			SqlYearToMonth ytm;
			if (!SqlYearToMonth.TryParse(value.Value, out ytm))
				return SqlNull.Value;

			return ytm;
		}
	}
}