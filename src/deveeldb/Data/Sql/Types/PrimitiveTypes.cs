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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;

namespace Deveel.Data.Sql.Types {
	/// <summary>
	/// Provides some helper functions for resolving and creating
	/// <see cref="SqlType"/> instances that are primitive to the
	/// system.
	/// </summary>
	public static class PrimitiveTypes {
		static PrimitiveTypes() {
			Resolver = new PrimitiveTypesResolver();
		}

		public static ISqlTypeResolver Resolver { get; }

		#region Boolean Types

		public static SqlBooleanType Boolean() {
			return Boolean(SqlTypeCode.Boolean);
		}

		public static SqlBooleanType Boolean(SqlTypeCode sqlType) {
			return new SqlBooleanType(sqlType);
		}

		public static SqlBooleanType Bit() {
			return Boolean(SqlTypeCode.Bit);
		}
		#endregion

		#region Binary Types

		public static SqlBinaryType Binary(int maxSize) {
			return Binary(SqlTypeCode.Binary, maxSize);
		}

		public static SqlBinaryType Binary(SqlTypeCode sqlType, int maxSize) {
			return new SqlBinaryType(sqlType, maxSize);
		}

		public static SqlBinaryType VarBinary() {
			return VarBinary(-1);
		}

		public static SqlBinaryType VarBinary(int maxSize) {
			return Binary(SqlTypeCode.VarBinary, maxSize);
		}

	    public static SqlBinaryType Blob() {
	        return Blob(-1);
	    }

	    public static SqlBinaryType Blob(int size) {
			return Binary(SqlTypeCode.Blob, size);
		}

	    public static SqlBinaryType LongVarBinary()
	        => Binary(SqlTypeCode.LongVarBinary, -1);

		#endregion

		#region Numeric Types

		public static SqlNumericType Numeric(int precision, int scale) {
			return Numeric(SqlTypeCode.Numeric, precision, scale);
		}

		public static SqlNumericType Numeric(SqlTypeCode sqlType, int precision, int scale) {
			return new SqlNumericType(sqlType, precision, scale);
		}

		public static SqlNumericType VarNumeric() {
			return Numeric(SqlTypeCode.VarNumeric, -1, -1);
		}

		public static SqlNumericType TinyInt() {
			return Numeric(SqlTypeCode.TinyInt, SqlNumericType.TinyIntPrecision, 0);
		}

		public static SqlNumericType SmallInt() {
			return Numeric(SqlTypeCode.SmallInt, SqlNumericType.SmallIntPrecision, 0);
		}

		public static SqlNumericType Integer() {
			return Numeric(SqlTypeCode.Integer, SqlNumericType.IntegerPrecision, 0);
		}

		public static SqlNumericType BigInt() {
			return Numeric(SqlTypeCode.BigInt, SqlNumericType.BigIntPrecision, 0);
		}

		public static SqlNumericType Float() {
			return Numeric(SqlTypeCode.Float, SqlNumericType.FloatPrecision, -1);
		}

		public static SqlNumericType Double() {
			return Numeric(SqlTypeCode.Double, SqlNumericType.DoublePrecision, -1);
		}

		public static SqlNumericType Decimal() {
			return Numeric(SqlTypeCode.Decimal, SqlNumericType.DecimalPrecision, -1);
		}

		#endregion

		#region String Types

		public static SqlCharacterType String() {
			return String(SqlTypeCode.String);
		}

		public static SqlCharacterType String(SqlTypeCode sqlType) {
			return String(sqlType, -1);
		}

		public static SqlCharacterType String(SqlTypeCode sqlType, int maxSize) {
			return String(sqlType, maxSize, null);
		}

		public static SqlCharacterType String(SqlTypeCode sqlType, int maxSize, CultureInfo locale) {
			return new SqlCharacterType(sqlType, maxSize, locale);
		}

		public static SqlCharacterType VarChar(int maxSize) {
			return VarChar(maxSize, null);
		}

		public static SqlCharacterType VarChar() {
			return VarChar(null);
		}

		public static SqlCharacterType VarChar(CultureInfo locale) {
			return VarChar(-1, locale);
		}

		public static SqlCharacterType VarChar(int maxSize, CultureInfo locale) {
			return String(SqlTypeCode.VarChar, maxSize, locale);
		}

		public static SqlCharacterType Char(int size) {
			return Char(size, null);
		}

		public static SqlCharacterType Char(int size, CultureInfo locale) {
			return String(SqlTypeCode.Char, size, locale);
		}

		public static SqlCharacterType Clob(int size) {
			return String(SqlTypeCode.Clob, size);
		}

	    public static SqlCharacterType Clob() {
	        return Clob(-1);
	    }

        public static SqlCharacterType LongVarChar() {
	        return String(SqlTypeCode.LongVarChar, -1);
	    }

		#endregion

		#region Date Types

	    public static SqlDateTimeType DateTime() {
	        return DateTime(SqlTypeCode.DateTime);
	    }

	    public static SqlDateTimeType DateTime(SqlTypeCode typeCode) {
			return new SqlDateTimeType(typeCode);
		}

		public static SqlDateTimeType TimeStamp() {
			return DateTime(SqlTypeCode.TimeStamp);
		}

		public static SqlDateTimeType Time() {
			return DateTime(SqlTypeCode.Time);
		}

		public static SqlDateTimeType Date() {
			return DateTime(SqlTypeCode.Date);
		}

		#endregion

		#region Interval Types

		public static SqlYearToMonthType YearToMonth() {
			return new SqlYearToMonthType();
		}

		public static SqlDayToSecondType DayToSecond() {
			return new SqlDayToSecondType();
		}

		#endregion

		#region Array

		// TODO:
		//public static SqlArrayType Array(int length) {
		//	return new SqlArrayType(length);
		//}

		#endregion

		/// <summary>
		/// Checks if the given code represents a primitive type.
		/// </summary>
		/// <param name="sqlType">The type code to check</param>
		/// <returns>
		/// Returns <c>true</c> of the given type code represents
		/// a primitive type, otherwise it returns <c>false</c>.
		/// </returns>
		public static bool IsPrimitive(SqlTypeCode sqlType) {
			if (sqlType == SqlTypeCode.Unknown ||
			    sqlType == SqlTypeCode.Type ||
			    sqlType == SqlTypeCode.QueryPlan ||
			    sqlType == SqlTypeCode.Object ||
				sqlType == SqlTypeCode.RowRef ||
				sqlType == SqlTypeCode.FieldRef)
				return false;

			return true;
		}

		/// <summary>
		/// Checks if the string represents the name of a primitive type
		/// </summary>
		/// <param name="name">The name to check</param>
		/// <returns>
		/// Returns <c>true</c> if the input <paramref name="name"/> represents
		/// a primitive type, otherwise returns <c>false</c>.
		/// </returns>
		public static bool IsPrimitive(string name) {
			if (System.String.IsNullOrEmpty(name))
				return false;

			switch (name.ToUpperInvariant()) {
				case "NULL":
					return true;

				case "BOOLEAN":
				case "BOOL":
				case "BIT":
					return true;

				case "NUMERIC":
				case "INT":
				case "INTEGER":
				case "BIGINT":
				case "TINYINT":
				case "SMALLINT":
				case "REAL":
				case "FLOAT":
				case "DOUBLE":
				case "DECIMAL":
				case "VARNUMERIC":
				case "NUMERIC VARYING":
					return true;

				case "STRING":
				case "VARCHAR":
				case "CHAR":
				case "CHARACTER VARYING":
				case "CLOB":
				case "LONGVARCHAR":
				case "LONG VARCHAR":
				case "LONG CHARACTER VARYING":
				case "TEXT":
					return true;

				case "BINARY":
				case "VARBINARY":
				case "LONG VARBINARY":
				case "LONGVARBINARY":
				case "LONG BINARY VARYING":
				case "BLOB":
					return true;

				case "DATE":
				case "DATETIME":
				case "TIME":
				case "TIMESTAMP":
					return true;

				case "YEAR TO MONTH":
				case "DAY TO SECOND":
					return true;
			}

			return false;
		}

		private static string GetTypeName(SqlTypeCode typeCode) {
			if (!IsPrimitive(typeCode))
				throw new ArgumentException($"The type with code {typeCode} is not primitive");

			switch (typeCode) {
				case SqlTypeCode.LongVarChar:
					return "LONG VARCHAR";
				case SqlTypeCode.LongVarBinary:
					return "LONG VARBINARY";
				case SqlTypeCode.YearToMonth:
					return "YEAR TO MONTH";
				case SqlTypeCode.DayToSecond:
					return "DAY TO SECOND";
				default:
					return typeCode.ToString().ToUpperInvariant();
			}
		}

		private static SqlType ResolvePrimitive(SqlTypeResolveInfo resolveInfo) {
			if (resolveInfo == null)
				throw new ArgumentNullException(nameof(resolveInfo));

			if (!IsPrimitive(resolveInfo.TypeName))
				return null;

			switch (resolveInfo.TypeName.ToUpperInvariant()) {
				// Booleans
				case "BIT":
					return Bit();
				case "BOOL":
				case "BOOLEAN":
					return Boolean();

				// Numerics
				case "TINYINT":
					return TinyInt();
				case "SMALLINT":
					return SmallInt();
				case "INT":
				case "INTEGER":
					return Integer();
				case "BIGINT":
					return BigInt();
				case "REAL":
				case "FLOAT":
					return Float();
				case "DOUBLE":
					return Double();
				case "DECIMAL":
					return Decimal();
				case "NUMBER":
				case "NUMERIC": {
					var precision = resolveInfo.Properties.GetValue<int?>("Precision");
					var scale = resolveInfo.Properties.GetValue<int?>("Scale");

					if (precision == null)
						throw new ArgumentException("No precision specified to resolve NUMERIC");
					if (scale == null)
						throw new ArgumentException("No scale specified to resolve NUMERIC");

					return Numeric(precision.Value, scale.Value);
				}
				case "NUMERIC VARYING":
				case "VARNUMERIC":
					return VarNumeric();

				// Strings
				case "CHAR": {
					var size = resolveInfo.Properties.GetValue<int?>("Size") ?? SqlCharacterType.DefaultMaxSize;
					var localeString = resolveInfo.Properties.GetValue<string>("Locale");
					var locale = System.String.IsNullOrEmpty(localeString) ? null : new CultureInfo(localeString);
					return Char(size, locale);
				}
				case "VARCHAR":
				case "CHARACTER VARYING":
				case "STRING": {
					var maxSize = resolveInfo.Properties.GetValue<int?>("MaxSize") ?? -1;
					var localeString = resolveInfo.Properties.GetValue<string>("Locale");
					var locale = System.String.IsNullOrEmpty(localeString) ? null : new CultureInfo(localeString);
				    var typeCode = System.String.Equals(resolveInfo.TypeName, "STRING", StringComparison.OrdinalIgnoreCase)
				        ? SqlTypeCode.String
				        : SqlTypeCode.VarChar;

				    return String(typeCode, maxSize, locale);
				}
			    case "LONG VARCHAR":
			    case "LONGVARCHAR":
			    case "LONG CHARACTER VARYING":
			        return LongVarChar();
			    case "TEXT":
			    case "CLOB": {
			        var size = resolveInfo.Properties.GetValue<int?>("Size") ?? -1;
                    return Clob(size);
			    }

			    // Date-Time
				case "DATE":
					return Date();
				case "DATETIME":
				    return DateTime();
				case "TIMESTAMP":
					return TimeStamp();
				case "TIME":
					return Time();

				// Intervals
				case "DAY TO SECOND":
					return DayToSecond();
				case "YEAR TO MONTH":
					return YearToMonth();

				// Binary
				case "BINARY": {
					var size = resolveInfo.Properties.GetValue<int?>("Size") ?? SqlBinaryType.DefaultMaxSize;
					return Binary(size);
				}
				case "VARBINARY":
				case "BINARY VARYING": {
					var size = resolveInfo.Properties.GetValue<int?>("MaxSize") ?? -1;
					return VarBinary(size);
				}
			    case "LONGVARBINARY":
			    case "LONG VARBINARY":
			    case "LONG BINARY VARYING":
			        return LongVarBinary();
				case "BLOB": {
				    var size = resolveInfo.Properties.GetValue<int?>("Size") ?? -1;
                    return Blob(size);
				}

				default:
					return null;
			}
		}

		public static SqlType Type(string typeName) {
			return Type(typeName, null);
		}

		public static SqlType Type(string typeName, IDictionary<string, object> properties) {
			return Resolver.Resolve(new SqlTypeResolveInfo(typeName, properties));
		}

		public static SqlType Type(SqlTypeCode typeCode) {
			return Type(typeCode, null);
		}

		public static SqlType Type(SqlTypeCode typeCode, IDictionary<string, object> propeties) {
			var typeName = GetTypeName(typeCode);
			return Type(typeName, propeties);
		}

		public static SqlType Type(SqlTypeCode typeCode, object properties) {
			var dictionary = properties == null
				? new Dictionary<string, object>()
				: properties.GetType()
					.GetRuntimeProperties()
					.Select(x => new {key = x.Name, value = x.GetValue(properties)})
					.ToDictionary(x => x.key, y => y.value, StringComparer.OrdinalIgnoreCase);
			return Type(typeCode, dictionary);
		}

		#region PrimitiveTypesResolver

		class PrimitiveTypesResolver : ISqlTypeResolver {
			public SqlType Resolve(SqlTypeResolveInfo resolveInfo) {
				return ResolvePrimitive(resolveInfo);
			}
		}

		#endregion
	}
}