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
using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// Utility for converting to and from <see cref="DbType"/> objects.
	/// </summary>
	public static class TypeUtil {

		/// <summary>
		/// Converts from a <see cref="Type"/> object to a type as specified in <see cref="DbType"/>.
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public static DbType ToDbType(Type type) {
			if (type == typeof (String))
				return DbType.String;
			if (type == typeof (BigDecimal))
				return DbType.Numeric;
			if (type == typeof (DateTime))
				return DbType.Time;
			if (type == typeof (Boolean))
				return DbType.Boolean;
			if (type == typeof (ByteLongObject))
				return DbType.Blob;
			return DbType.Object;
		}

		/// <summary>
		/// Converts from a db type to a <see cref="Type"/> object.
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public static Type ToType(DbType type) {
			if (type == DbType.String)
				return typeof(String);
			if (type == DbType.Numeric)
				return typeof(BigDecimal);
			if (type == DbType.Time)
				return typeof(DateTime);
			if (type == DbType.Boolean)
				return typeof(Boolean);
			if (type == DbType.Blob)
				return typeof(ByteLongObject);
			if (type == DbType.Object)
				return typeof(Object);
			throw new ApplicationException("Unknown type.");
		}

		public static Type ToRuntimeType(SqlType sqlType) {
			if (sqlType == SqlType.Bit)
				return typeof (bool);

			if (sqlType == SqlType.TinyInt)
				return typeof (byte);
			if (sqlType == SqlType.SmallInt)
				return typeof (short);
			if (sqlType == SqlType.Integer)
				return typeof (int);
			if (sqlType == SqlType.BigInt)
				return typeof (long);
			if (sqlType == SqlType.Float ||
				sqlType == SqlType.Real)
				return typeof (float);
			if (sqlType == SqlType.Double)
				return typeof (double);

			// TODO: This is a temporary solution: needs to be investigated better
			if (sqlType == SqlType.Numeric)
				return typeof (BigNumber);

			if (sqlType == SqlType.Time ||
			    sqlType == SqlType.TimeStamp ||
			    sqlType == SqlType.Date)
				return typeof (DateTime);

			if (sqlType == SqlType.Interval)
				return typeof (TimeSpan);

			if (sqlType == SqlType.VarChar ||
			    sqlType == SqlType.Char)
				return typeof (string);

			if (sqlType == SqlType.Null)
				return typeof (DBNull);

			if (sqlType == SqlType.Unknown)
				return typeof (object);

			throw new ArgumentException(String.Format("Cannot convert SQL Type {0} to .NET Type", sqlType));
		}
	}
}