// 
//  Copyright 2010-2014 Deveel
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

using System;
using System.Globalization;

namespace Deveel.Data.Types {
	public static class PrimitiveTypes {
		public static BooleanType Boolean() {
			return Boolean(SqlTypeCode.Boolean);
		}

		public static BooleanType Boolean(SqlTypeCode sqlType) {
			return new BooleanType(sqlType);
		}

		public static StringType String(int maxSize) {
			return new StringType(SqlTypeCode.String, maxSize);
		}

		public static StringType String() {
			return String(SqlTypeCode.String);
		}

		public static StringType String(SqlTypeCode sqlType) {
			return String(sqlType, StringType.DefaultMaxSize);
		}

		public static StringType String(SqlTypeCode sqlType, int maxSize) {
			return new StringType(sqlType, maxSize);
		}

		public static StringType String(SqlTypeCode sqlType, CultureInfo locale) {
			return String(sqlType, StringType.DefaultMaxSize, locale);
		}

		public static StringType String(SqlTypeCode sqlType, int maxSize, CultureInfo locale) {
			return new StringType(sqlType, maxSize, locale);
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

		public static NumericType Numeric(SqlTypeCode sqlType, int size) {
			return Numeric(sqlType, size, 0);
		}

		public static NumericType Numeric(SqlTypeCode sqlType, int size, byte scale) {
			return new NumericType(sqlType, size, scale);
		}

		public static NullType Null() {
			return Null(SqlTypeCode.Null);
		}

		public static NullType Null(SqlTypeCode sqlType) {
			return new NullType(sqlType);
		}

		public static DateType Date() {
			return Date(SqlTypeCode.TimeStamp);
		}

		public static DateType Date(SqlTypeCode sqlType) {
			return new DateType(sqlType);
		}

		public static RowType RowType(ObjectName tableName) {
			return new RowType(tableName);
		}

		public static ColumnType ColumnType(ObjectName columnName) {
			return new ColumnType(columnName);
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

		public static bool IsPrimitive(SqlTypeCode sqlType) {
			if (sqlType == SqlTypeCode.Numeric ||
				sqlType == SqlTypeCode.TinyInt ||
				sqlType == SqlTypeCode.SmallInt ||
				sqlType == SqlTypeCode.Integer ||
				sqlType == SqlTypeCode.BigInt ||
				sqlType == SqlTypeCode.Real ||
				sqlType == SqlTypeCode.Double ||
				sqlType == SqlTypeCode.Float ||
				sqlType == SqlTypeCode.Decimal)
				return true;

			if (sqlType == SqlTypeCode.BigInt ||
				sqlType == SqlTypeCode.Boolean)
				return true;

			if (sqlType == SqlTypeCode.Char ||
				sqlType == SqlTypeCode.VarChar ||
				sqlType == SqlTypeCode.LongVarChar ||
				sqlType == SqlTypeCode.String ||
				sqlType == SqlTypeCode.Clob)
				return true;

			if (sqlType == SqlTypeCode.Binary ||
				sqlType == SqlTypeCode.VarBinary ||
				sqlType == SqlTypeCode.LongVarBinary ||
				sqlType == SqlTypeCode.Blob)
				return true;

			return false;
		}

		public static bool IsPrimitive(string name) {
			if (System.String.IsNullOrEmpty(name))
				return false;

			if (name.EndsWith("%TYPE", StringComparison.InvariantCultureIgnoreCase) ||
				name.EndsWith("%ROWTYPE", StringComparison.InvariantCultureIgnoreCase))
				return true;

			// TODO: Support also the other SQL type names?
			if (name.Equals("NUMERIC", StringComparison.InvariantCultureIgnoreCase) ||
				name.Equals("STRING", StringComparison.InvariantCultureIgnoreCase) ||
				name.Equals("DATE", StringComparison.InvariantCultureIgnoreCase) ||
				name.Equals("NULL", StringComparison.InvariantCultureIgnoreCase) ||
				name.Equals("BOOLEAN", StringComparison.InvariantCultureIgnoreCase) ||
				name.Equals("BINARY", StringComparison.InvariantCultureIgnoreCase))
				return true;

			return false;
		}

		public static DataType Type(string typeName, params object[] args) {
			if (System.String.Equals("long varchar", typeName, StringComparison.OrdinalIgnoreCase))
				typeName = "longvarchar";
			if (System.String.Equals("long varbinary", typeName, StringComparison.OrdinalIgnoreCase))
				typeName = "longvarbinary";

			SqlTypeCode typeCode;
			try {
				typeCode = (SqlTypeCode) Enum.Parse(typeof (SqlTypeCode), typeName, true);
			} catch (Exception) {
				throw new ArgumentException(System.String.Format("The name {0} is not a valid SQL type.", typeName));
			}

			return Type(typeCode, args);
		}

		public static DataType Type(SqlTypeCode sqlType, params object[] args) {
			if (sqlType == SqlTypeCode.BigInt ||
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
				if (args == null || args.Length == 0)
					return Numeric(sqlType);
				if (args.Length == 1)
					return Numeric(sqlType, (int)args[0]);
				if (args.Length == 2)
					return Numeric(sqlType, (int)args[0], (byte)args[1]);

				throw new ArgumentException("Invalid numer of arguments for NUMERIC type");
			}

			if (sqlType == SqlTypeCode.Char ||
				sqlType == SqlTypeCode.VarChar ||
				sqlType == SqlTypeCode.LongVarChar ||
				sqlType == SqlTypeCode.String ||
				sqlType == SqlTypeCode.Clob) {
				if (args == null || args.Length == 0)
					return String(sqlType);
				if (args.Length == 1) {
					var arg = args[0];
					if (arg is int)
						return String(sqlType, (int) arg);
					if (arg is string)
						return String(sqlType, CultureInfo.GetCultureInfo((string) arg));
				}

				throw new ArgumentException("Invalid numer of arguments for NUMERIC type");
			}

			if (sqlType == SqlTypeCode.Binary ||
				sqlType == SqlTypeCode.VarBinary ||
				sqlType == SqlTypeCode.LongVarBinary ||
				sqlType == SqlTypeCode.Blob) {
				if (args == null || args.Length == 0)
					return Binary(sqlType);
				if (args.Length == 1)
					return Binary(sqlType, (int)args[0]);

				throw new ArgumentException("Invalid number of arguments for BINARY type");
			}

			if (sqlType == SqlTypeCode.Date ||
				sqlType == SqlTypeCode.Time ||
				sqlType == SqlTypeCode.TimeStamp)
				return Date(sqlType);

			if (sqlType == SqlTypeCode.Null)
				return Null(sqlType);

			if (sqlType == SqlTypeCode.RowType) {
				if (args == null || args.Length != 1)
					throw new ArgumentException("Invalid number of arguments for %ROWTYPE type");

				return RowType((ObjectName)args[0]);
			}

			if (sqlType == SqlTypeCode.ColumnType) {
				if (args == null || args.Length != 1)
					throw new ArgumentException("Invalid number of arguments for %TYPE type");

				return ColumnType((ObjectName)args[0]);
			}

			throw new ArgumentException(System.String.Format("The SQL type {0} is not primitive.", sqlType));
		}

		public static DataType Query() {
			return new QueryType();
		}

		public static DataType FromType(Type type) {
			if (type == typeof(bool))
				return Boolean();
			if (type == typeof(string))
				return String();

			throw new NotSupportedException();
		}

		public static IntervalType Interval(SqlTypeCode sqlType) {
			return new IntervalType(sqlType);
		}
	}
}