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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Types {
	public static class PrimitiveTypes {
		public static BooleanType Boolean() {
			return Boolean(SqlType.Boolean);
		}

		public static BooleanType Boolean(SqlType sqlType) {
			return new BooleanType(sqlType);
		}

		public static StringType String(int maxSize) {
			return new StringType(SqlType.String, maxSize);
		}

		public static StringType String() {
			return String(SqlType.String, -1);
		}

		public static StringType String(SqlType sqlType) {
			return String(sqlType, -1);
		}

		public static StringType String(SqlType sqlType, int maxSize) {
			return new StringType(sqlType, maxSize);
		}

		public static NumericType Numeric() {
			return Numeric(-1);
		}

		public static NumericType Numeric(int size) {
			return Numeric(size, 0);
		}

		public static NumericType Numeric(int size, byte scale) {
			return Numeric(SqlType.Numeric, size, scale);
		}

		public static NumericType Numeric(SqlType sqlType) {
			return Numeric(sqlType, -1);
		}

		public static NumericType Numeric(SqlType sqlType, int size) {
			return Numeric(sqlType, size, 0);
		}

		public static NumericType Numeric(SqlType sqlType, int size, byte scale) {
			return new NumericType(sqlType, size, scale);
		}

		public static NullType Null() {
			return Null(SqlType.Null);
		}

		public static NullType Null(SqlType sqlType) {
			return new NullType(sqlType);
		}

		public static DateType Date() {
			return Date(SqlType.TimeStamp);
		}

		public static DateType Date(SqlType sqlType) {
			return new DateType(sqlType);
		}

		public static RowType RowType(ObjectName tableName) {
			return new RowType(tableName);
		}

		public static ColumnType ColumnType(ObjectName columnName) {
			return new ColumnType(columnName);
		}

		public static BinaryType Binary(int maxSize) {
			return Binary(SqlType.Binary, maxSize);
		}

		public static BinaryType Binary() {
			return Binary(SqlType.Binary);
		}

		public static BinaryType Binary(SqlType sqlType) {
			return Binary(sqlType, -1);
		}

		public static BinaryType Binary(SqlType sqlType, int maxSize) {
			return new BinaryType(sqlType, maxSize);
		}

		public static bool IsPrimitive(SqlType sqlType) {
			if (sqlType == SqlType.Numeric ||
				sqlType == SqlType.TinyInt ||
				sqlType == SqlType.SmallInt ||
				sqlType == SqlType.Integer ||
				sqlType == SqlType.BigInt ||
				sqlType == SqlType.Real ||
				sqlType == SqlType.Double ||
				sqlType == SqlType.Float ||
				sqlType == SqlType.Decimal)
				return true;

			if (sqlType == SqlType.BigInt ||
				sqlType == SqlType.Boolean)
				return true;

			if (sqlType == SqlType.Char ||
				sqlType == SqlType.VarChar ||
				sqlType == SqlType.LongVarChar ||
				sqlType == SqlType.String ||
				sqlType == SqlType.Clob)
				return true;

			if (sqlType == SqlType.Binary ||
				sqlType == SqlType.VarBinary ||
				sqlType == SqlType.LongVarBinary ||
				sqlType == SqlType.Blob)
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

		public static DataType Type(SqlType sqlType, params object[] args) {
			if (sqlType == SqlType.BigInt ||
				sqlType == SqlType.Boolean)
				return Boolean(sqlType);

			if (sqlType == SqlType.Numeric ||
				sqlType == SqlType.TinyInt ||
				sqlType == SqlType.SmallInt ||
				sqlType == SqlType.Integer ||
				sqlType == SqlType.BigInt ||
				sqlType == SqlType.Real ||
				sqlType == SqlType.Double ||
				sqlType == SqlType.Float ||
				sqlType == SqlType.Decimal) {
				if (args == null || args.Length == 0)
					return Numeric(sqlType);
				if (args.Length == 1)
					return Numeric(sqlType, (int)args[0]);
				if (args.Length == 2)
					return Numeric(sqlType, (int)args[0], (byte)args[1]);

				throw new ArgumentException("Invalid numer of arguments for NUMERIC type");
			}

			if (sqlType == SqlType.Char ||
				sqlType == SqlType.VarChar ||
				sqlType == SqlType.LongVarChar ||
				sqlType == SqlType.String ||
				sqlType == SqlType.Clob) {
				if (args == null || args.Length == 0)
					return String(sqlType);
				if (args.Length == 1)
					return String(sqlType, (int)args[0]);

				throw new ArgumentException("Invalid numer of arguments for NUMERIC type");
			}

			if (sqlType == SqlType.Binary ||
				sqlType == SqlType.VarBinary ||
				sqlType == SqlType.LongVarBinary ||
				sqlType == SqlType.Blob) {
				if (args == null || args.Length == 0)
					return Binary(sqlType);
				if (args.Length == 1)
					return Binary(sqlType, (int)args[0]);

				throw new ArgumentException("Invalid number of arguments for BINARY type");
			}

			if (sqlType == SqlType.Date ||
				sqlType == SqlType.Time ||
				sqlType == SqlType.TimeStamp)
				return Date(sqlType);

			if (sqlType == SqlType.Null)
				return Null(sqlType);

			if (sqlType == SqlType.RowType) {
				if (args == null || args.Length != 1)
					throw new ArgumentException("Invalid number of arguments for %ROWTYPE type");

				return RowType((ObjectName)args[0]);
			}

			if (sqlType == SqlType.ColumnType) {
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

		public static IntervalType Interval(SqlType sqlType) {
			return new IntervalType(sqlType);
		}

		public static IntervalType Interval() {
			return Interval(SqlType.Interval);
		}
	}
}