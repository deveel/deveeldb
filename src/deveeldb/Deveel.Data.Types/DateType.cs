// 
//  Copyright 2010-2015 Deveel
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

using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Types {
	public sealed class DateType : DataType {
		public DateType(SqlTypeCode sqlType) 
			: base("DATE", sqlType) {
			AssertDateType(sqlType);
		}

		public static readonly string[] DateFormatSql = {
			"yyyy-MM-dd",
			"yyyy MM dd"
		};

		public static readonly string[] TimeFormatSql = {
			"HH:mm:ss.fff z",
			"HH:mm:ss.fff zz",
			"HH:mm:ss.fff zzz",
			"HH:mm:ss.fff",
			"HH:mm:ss z",
			"HH:mm:ss zz",
			"HH:mm:ss zzz",
			"HH:mm:ss"
		};

		public static readonly string[] TsFormatSql = {
			"yyyy-MM-dd HH:mm:ss.fff",
			"yyyy-MM-dd HH:mm:ss.fff z",
			"yyyy-MM-dd HH:mm:ss.fff zz",
			"yyyy-MM-dd HH:mm:ss.fff zzz",
			"yyyy-MM-dd HH:mm:ss",
			"yyyy-MM-dd HH:mm:ss z",
			"yyyy-MM-dd HH:mm:ss zz",
			"yyyy-MM-dd HH:mm:ss zzz",

			"yyyy-MM-ddTHH:mm:ss.fff",
			"yyyy-MM-ddTHH:mm:ss.fff z",
			"yyyy-MM-ddTHH:mm:ss.fff zz",
			"yyyy-MM-ddTHH:mm:ss.fff zzz",
			"yyyy-MM-ddTHH:mm:ss",
			"yyyy-MM-ddTHH:mm:ss z",
			"yyyy-MM-ddTHH:mm:ss zz",
			"yyyy-MM-ddTHH:mm:ss zzz",
		};


		private static void AssertDateType(SqlTypeCode sqlType) {
			if (!IsDateType(sqlType))
				throw new ArgumentException(String.Format("The SQL type {0} is not a valid DATE", sqlType), "sqlType");
		}

		public override bool IsStorable {
			get { return true; }
		}

		public override bool IsCacheable(ISqlObject value) {
			return value is SqlDateTime || value is SqlNull;
		}

		public override bool Equals(object obj) {
			var other = obj as DataType;
			if (other == null)
				return false;

			return SqlType == other.SqlType;
		}

		public override int GetHashCode() {
			return SqlType.GetHashCode();
		}

		public override bool CanCastTo(DataType destType) {
			return destType is StringType || destType is DateType;
		}

		private SqlString ToString(SqlDateTime dateTime) {
			if (dateTime.IsNull)
				return SqlString.Null;

			if (SqlType == SqlTypeCode.Date)
				return dateTime.ToDateString();
			if (SqlType == SqlTypeCode.Time)
				return dateTime.ToTimeString();
			if (SqlType == SqlTypeCode.TimeStamp)
				return dateTime.ToTimeStampString();

			return SqlString.Null;
		}

		public override DataObject CastTo(DataObject value, DataType destType) {
			if (destType == null)
				throw new ArgumentNullException("destType");

			var date = (SqlDateTime) value.Value;
			var sqlType = destType.SqlType;

			ISqlObject casted;

			switch (sqlType) {
				case SqlTypeCode.String:
				case SqlTypeCode.VarChar:
					casted = ToString(date);
					break;
				default:
					throw new InvalidCastException(String.Format("Cannot cast type '{0}' to '{1}'.",
						sqlType.ToString().ToUpperInvariant(), SqlType.ToString().ToUpperInvariant()));
			}

			return new DataObject(destType, casted);
		}

		public override void SerializeObject(Stream stream, ISqlObject obj) {
			var writer = new BinaryWriter(stream);

			if (obj is SqlNull) {
				writer.Write((byte) 0);
			} else {
				var date = (SqlDateTime) obj;

				if (date.IsNull) {
					writer.Write((byte)0);
				} else {
					var bytes = date.ToByteArray(true);

					writer.Write((byte)1);
					writer.Write(bytes.Length);
					writer.Write(bytes);
				}
			}
		}

		internal override int ColumnSizeOf(ISqlObject obj) {
			if (obj is SqlNull)
				return 1;

			if (!(obj is SqlDateTime))
				throw new ArgumentException(String.Format("Cannot determine the size of an object of type '{0}'", obj.GetType()));

			if (obj.IsNull)
				return 1;

			// Type + Length + Bytes
			return 1 + 4 + 13;
		}

		public override ISqlObject DeserializeObject(Stream stream) {
			var reader = new BinaryReader(stream);

			var type = reader.ReadByte();
			if (type == 0)
				return SqlDateTime.Null;

			var length = reader.ReadInt32();
			var bytes = reader.ReadBytes(length);
			return new SqlDateTime(bytes);
		}

		internal static bool IsDateType(SqlTypeCode sqlType) {
			return sqlType == SqlTypeCode.Date ||
			       sqlType == SqlTypeCode.Time ||
			       sqlType == SqlTypeCode.TimeStamp ||
				   sqlType == SqlTypeCode.DateTime;
		}
	}
}