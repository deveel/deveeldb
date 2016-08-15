// 
//  Copyright 2010-2016 Deveel
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
using System.Text;

using Deveel.Data.Sql;
using Deveel.Math;

namespace Deveel.Data.Sql.Types {
	/// <summary>
	/// Provides some helper functions for resolving and creating
	/// <see cref="SqlType"/> instances that are primitive to the
	/// system.
	/// </summary>
	public static class PrimitiveTypes {
		public static BooleanType Boolean() {
			return Boolean(SqlTypeCode.Boolean);
		}

		public static BooleanType Boolean(SqlTypeCode sqlType) {
			return new BooleanType(sqlType);
		}

		public static BooleanType Bit() {
			return Boolean(SqlTypeCode.Bit);
		}

		public static StringType String() {
			return String(SqlTypeCode.String);
		}

		public static StringType String(SqlTypeCode sqlType) {
			return String(sqlType, -1);
		}

		public static StringType String(SqlTypeCode sqlType, int maxSize) {
			return String(sqlType, maxSize, (CultureInfo) null);
		}

		public static StringType String(SqlTypeCode sqlType, CultureInfo locale) {
			return String(sqlType, -1, locale);
		}

		public static StringType String(SqlTypeCode sqlType, int maxSize, CultureInfo locale) {
			return String(sqlType, maxSize, Encoding.Unicode, locale);
		}

		public static StringType String(SqlTypeCode sqlType, Encoding encoding) {
			return String(sqlType, -1, encoding);
		}

		public static StringType String(SqlTypeCode sqlType, int maxSize, Encoding encoding) {
			return String(sqlType, maxSize, encoding, null);
		}

		public static StringType String(SqlTypeCode sqlType, Encoding encoding, CultureInfo locale) {
			return String(sqlType, -1, encoding, locale);
		}

		public static StringType String(SqlTypeCode sqlType, int maxSize, Encoding encoding, CultureInfo locale) {
			return new StringType(sqlType, maxSize, encoding, locale);
		}

		public static StringType Clob() {
			return Clob(Int16.MaxValue);
		}

		public static StringType Clob(int maxSize) {
			return Clob(Encoding.ASCII, maxSize);
		}

		public static StringType Clob(Encoding encoding) {
			return Clob(encoding, Int16.MaxValue);
		}

		public static StringType Clob(Encoding encoding, int maxSize) {
			return String(SqlTypeCode.Clob, maxSize, encoding);
		}

		public static StringType VarChar() {
			return VarChar(-1);
		}

		public static StringType VarChar(int maxSize) {
			return VarChar(maxSize, Encoding.Unicode);
		}

		public static StringType VarChar(int maxSize, Encoding encoding) {
			return VarChar(maxSize, encoding, null);
		}

		public static StringType VarChar(Encoding encoding) {
			return VarChar(encoding, null);
		}

		public static StringType VarChar(CultureInfo locale) {
			return VarChar(Encoding.Unicode, locale);
		}

		public static StringType VarChar(Encoding encoding, CultureInfo locale) {
			return VarChar(-1, encoding, locale);
		}

		public static StringType VarChar(int maxSize, Encoding encoding, CultureInfo locale) {
			return String(SqlTypeCode.VarChar, maxSize, encoding, locale);
		}

		public static NumericType Numeric() {
			return Numeric(-1);
		}

		public static NumericType Numeric(int size) {
			return Numeric(size, 0);
		}

		public static NumericType Numeric(int size, byte scale) {
			return Numeric(SqlTypeCode.Numeric, size, scale);
		}

		public static NumericType Numeric(SqlTypeCode sqlType) {
			return Numeric(sqlType, -1);
		}

		public static NumericType Numeric(SqlTypeCode sqlType, int precision) {
			return Numeric(sqlType, precision, 0);
		}

		public static NumericType Numeric(SqlTypeCode sqlType, int precision, int scale) {
			return new NumericType(sqlType, precision, scale);
		}

		public static NumericType TinyInt(int size) {
			return Numeric(SqlTypeCode.TinyInt, size);
		}

		public static NumericType TinyInt() {
			return TinyInt(-1);
		}

		public static NumericType Integer() {
			return Integer(-1);
		}

		public static NumericType Integer(int size) {
			return Numeric(SqlTypeCode.Integer, size);
		}

		public static SqlType BigInt() {
			return BigInt(-1);
		}

		public static SqlType BigInt(int size) {
			return Numeric(SqlTypeCode.BigInt, size);
		}

		public static NumericType Real() {
			return Real(-1);
		}

		public static NumericType Real(int precision) {
			return Numeric(SqlTypeCode.Real, precision);
		}

		public static NumericType Real(int precision, byte scale) {
			return Numeric(SqlTypeCode.Real, precision, scale);
		}

		public static NullType Null() {
			return Null(SqlTypeCode.Null);
		}

		public static NullType Null(SqlTypeCode sqlType) {
			return new NullType(sqlType);
		}

		public static DateType DateTime() {
			return DateTime(SqlTypeCode.DateTime);
		}

		public static DateType DateTime(SqlTypeCode sqlType) {
			return new DateType(sqlType);
		}

		public static DateType Date() {
			return DateTime(SqlTypeCode.Date);
		}

		public static DateType TimeStamp() {
			return DateTime(SqlTypeCode.TimeStamp);
		}

		public static DateType Time() {
			return DateTime(SqlTypeCode.Time);
		}

		public static BinaryType Binary(int maxSize) {
			return Binary(SqlTypeCode.Binary, maxSize);
		}

		public static BinaryType Binary() {
			return Binary(SqlTypeCode.Binary);
		}

		public static BinaryType Binary(SqlTypeCode sqlType) {
			return Binary(sqlType, -1);
		}

		public static BinaryType Binary(SqlTypeCode sqlType, int maxSize) {
			return new BinaryType(sqlType, maxSize);
		}

		public static BinaryType Blob(int maxSize) {
			return Binary(SqlTypeCode.Blob, maxSize);
		}

		public static IntervalType Interval(SqlTypeCode sqlType) {
			return new IntervalType(sqlType);
		}

		public static IntervalType DayToSecond() {
			return Interval(SqlTypeCode.DayToSecond);
		}

		public static IntervalType YearToMonth() {
			return Interval(SqlTypeCode.YearToMonth);
		}

		public static bool IsPrimitive(SqlTypeCode sqlType) {
			if (sqlType == SqlTypeCode.Unknown ||
			    sqlType == SqlTypeCode.Type ||
			    sqlType == SqlTypeCode.QueryPlan ||
			    sqlType == SqlTypeCode.Object)
				return false;

			return true;
		}

		public static bool IsPrimitive(string name) {
			if (System.String.IsNullOrEmpty(name))
				return false;

			if (System.String.Equals("long varchar", name, StringComparison.OrdinalIgnoreCase))
				name = "longvarchar";
			if (System.String.Equals("long varbinary", name, StringComparison.OrdinalIgnoreCase))
				name = "longvarbinary";

			if (name.EndsWith("%TYPE", StringComparison.OrdinalIgnoreCase) ||
				name.EndsWith("%ROWTYPE", StringComparison.OrdinalIgnoreCase))
				return true;

			if (name.Equals("NUMERIC", StringComparison.OrdinalIgnoreCase) ||
				name.Equals("STRING", StringComparison.OrdinalIgnoreCase) ||
				name.Equals("DATE", StringComparison.OrdinalIgnoreCase) ||
				name.Equals("NULL", StringComparison.OrdinalIgnoreCase) ||
				name.Equals("BOOLEAN", StringComparison.OrdinalIgnoreCase) ||
				name.Equals("BINARY", StringComparison.OrdinalIgnoreCase))
				return true;

			if (name.Equals("BIT", StringComparison.OrdinalIgnoreCase) ||
			    name.Equals("BOOLEAN", StringComparison.OrdinalIgnoreCase))
				return true;

			if (name.Equals("TINYINT", StringComparison.OrdinalIgnoreCase) ||
			    name.Equals("SMALLINT", StringComparison.OrdinalIgnoreCase) ||
			    name.Equals("INTEGER", StringComparison.OrdinalIgnoreCase) ||
			    name.Equals("INT", StringComparison.OrdinalIgnoreCase) ||
			    name.Equals("BIGINT", StringComparison.OrdinalIgnoreCase) ||
				name.Equals("REAL", StringComparison.OrdinalIgnoreCase) ||
				name.Equals("FLOAT", StringComparison.OrdinalIgnoreCase) ||
				name.Equals("DOUBLE", StringComparison.OrdinalIgnoreCase) ||
				name.Equals("DECIMAL", StringComparison.OrdinalIgnoreCase))
				return true;

			if (name.Equals("DATE", StringComparison.OrdinalIgnoreCase) ||
			    name.Equals("TIME", StringComparison.OrdinalIgnoreCase) ||
			    name.Equals("TIMESTAMP", StringComparison.OrdinalIgnoreCase) ||
				name.Equals("DATETIME", StringComparison.OrdinalIgnoreCase))
				return true;

			if (name.Equals("YEAR TO MONTH", StringComparison.OrdinalIgnoreCase) ||
			    name.Equals("DAY TO SECOND", StringComparison.OrdinalIgnoreCase) ||
				name.Equals("INTERVAL", StringComparison.OrdinalIgnoreCase))
				return true;

			if (name.Equals("CHAR", StringComparison.OrdinalIgnoreCase) ||
			    name.Equals("VARCHAR", StringComparison.OrdinalIgnoreCase) ||
			    name.Equals("LONGVARCHAR", StringComparison.OrdinalIgnoreCase) ||
				name.Equals("CLOB", StringComparison.OrdinalIgnoreCase))
				return true;

			if (name.Equals("BINARY", StringComparison.OrdinalIgnoreCase) ||
			    name.Equals("VARBINARY", StringComparison.OrdinalIgnoreCase) ||
			    name.Equals("LONGVARBINARY", StringComparison.OrdinalIgnoreCase) ||
				name.Equals("BLOB", StringComparison.OrdinalIgnoreCase))
				return true;

			return false;
		}

		public static SqlType Resolve(string typeName, params DataTypeMeta[] args) {
			if (!IsPrimitive(typeName))
				return Null();

			if (System.String.Equals("long varchar", typeName, StringComparison.OrdinalIgnoreCase))
				typeName = "longvarchar";
			if (System.String.Equals("long varbinary", typeName, StringComparison.OrdinalIgnoreCase))
				typeName = "longvarbinary";
			if (System.String.Equals("day to second", typeName, StringComparison.OrdinalIgnoreCase))
				typeName = "daytosecond";
			if (System.String.Equals("year to month", typeName, StringComparison.OrdinalIgnoreCase))
				typeName = "yeartomonth";
			if (System.String.Equals("%rowtype", typeName, StringComparison.OrdinalIgnoreCase))
				typeName = "rowref";
			if (System.String.Equals("%type", typeName, StringComparison.OrdinalIgnoreCase))
				typeName = "fieldref";

			SqlTypeCode typeCode;

			try {
				typeCode = (SqlTypeCode) Enum.Parse(typeof (SqlTypeCode), typeName, true);
			} catch (Exception) {
				throw new ArgumentException(System.String.Format("The name {0} is not a valid SQL type.", typeName));
			}

			return Resolve(typeCode, typeName, args);
		}

		public static SqlType Resolve(SqlTypeCode sqlType, string typeName, params DataTypeMeta[] args) {
			return Resolve(new TypeResolveContext(sqlType, typeName, args));
		}

		public static SqlType Resolve(TypeResolveContext context) {
			if (!context.IsPrimitive)
				return null;

			var sqlType = context.TypeCode;

			if (sqlType == SqlTypeCode.Bit ||
			    sqlType == SqlTypeCode.Boolean)
				return Boolean(sqlType);

			if (sqlType == SqlTypeCode.Numeric ||
			    sqlType == SqlTypeCode.TinyInt ||
			    sqlType == SqlTypeCode.SmallInt ||
			    sqlType == SqlTypeCode.Integer ||
			    sqlType == SqlTypeCode.BigInt ||
			    sqlType == SqlTypeCode.Real ||
			    sqlType == SqlTypeCode.Double ||
			    sqlType == SqlTypeCode.Float ||
			    sqlType == SqlTypeCode.Decimal) {
				if (!context.HasAnyMeta)
					return Numeric(sqlType);

				var precisionMeta = context.GetMeta("Precision");
				var scaleMeta = context.GetMeta("Scale");

				if (precisionMeta == null)
					return Numeric(sqlType);

				if (scaleMeta == null)
					return Numeric(sqlType, precisionMeta.ToInt32());

				return Numeric(sqlType, precisionMeta.ToInt32(), (byte) scaleMeta.ToInt32());
			}

			if (sqlType == SqlTypeCode.Char ||
			    sqlType == SqlTypeCode.VarChar ||
			    sqlType == SqlTypeCode.LongVarChar ||
			    sqlType == SqlTypeCode.String ||
			    sqlType == SqlTypeCode.Clob) {
				if (!context.HasAnyMeta)
					return String(sqlType);

				var maxSizeMeta = context.GetMeta("MaxSize");
				var localeMeta = context.GetMeta("Locale");
				var encodingMeta = context.GetMeta("Encoding");

				int maxSize = -1;
				CultureInfo locale = null;
				var encoding = Encoding.Unicode;

				if (maxSizeMeta != null) {
					if (maxSizeMeta.Value == "MAX") {
						maxSize = StringType.DefaultMaxSize;
					} else {
						maxSize = maxSizeMeta.ToInt32();
					}
				}

				if (localeMeta != null)
					locale = new CultureInfo(localeMeta.Value);
				if (encodingMeta != null)
					encoding = Encoding.GetEncoding(encodingMeta.Value);

				return new StringType(sqlType, maxSize, encoding, locale);
			}

			if (sqlType == SqlTypeCode.Binary ||
			    sqlType == SqlTypeCode.VarBinary ||
			    sqlType == SqlTypeCode.LongVarBinary ||
			    sqlType == SqlTypeCode.Blob) {
				if (!context.HasAnyMeta)
					return Binary(sqlType);

				var maxSize = BinaryType.DefaultMaxSize;
				var maxSizeMeta = context.GetMeta("MaxSize");
				if (maxSizeMeta != null) {
					if (maxSizeMeta.Value == "MAX") {
						maxSize = BinaryType.DefaultMaxSize;
					} else {
						maxSize = maxSizeMeta.ToInt32();
					}
				}

				return Binary(sqlType, maxSize);
			}

			if (sqlType == SqlTypeCode.Date ||
			    sqlType == SqlTypeCode.Time ||
			    sqlType == SqlTypeCode.TimeStamp ||
			    sqlType == SqlTypeCode.DateTime)
				return DateTime(sqlType);

			if (sqlType == SqlTypeCode.YearToMonth ||
			    sqlType == SqlTypeCode.DayToSecond)
				return Interval(sqlType);

			if (sqlType == SqlTypeCode.Null)
				return Null(sqlType);

			// Ref types
			if (sqlType == SqlTypeCode.FieldRef) {
				var meta = context.GetMeta("FieldName");
				if (meta == null)
					throw new InvalidOperationException("Invalid construction of a %TYPE reference");

				var fieldName = ObjectName.Parse(meta.Value);
				return new FieldRefType(fieldName);
			}

			if (sqlType == SqlTypeCode.RowRef) {
				var meta = context.GetMeta("ObjectName");
				if (meta == null)
					throw new InvalidOperationException("Invalid construction of a %ROWTYPE reference");

				var objName = ObjectName.Parse(meta.Value);
				return new RowRefType(objName);
			}

			throw new ArgumentException(System.String.Format("The SQL type {0} is not primitive.", sqlType));
		}

		public static SqlType FromType(Type type) {
			if (type == typeof(bool))
				return Boolean();
			if (type == typeof(string))
				return String();
			if (type == typeof (byte))
				return Bit();
			if (type == typeof (short))
				return TinyInt();
			if (type == typeof (int))
				return Integer();
			if (type == typeof (long))
				return BigInt();
			if (type == typeof (float) ||
			    type == typeof (double))
				return Real();
			if (type == typeof (BigInteger))
				return Numeric();
			if (type == typeof (DateTime))
				return TimeStamp();

			throw new NotSupportedException(System.String.Format("The runtime type '{0}' is not supported as SQL type (yet).", type));
		}
	}
}