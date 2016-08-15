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
using System.IO;

using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Sql.Types {
	[Serializable]
	public sealed class IntervalType : SqlType {
		public IntervalType(SqlTypeCode typeCode) 
			: base(GetTypeString(typeCode), typeCode) {
			AssertIsInterval(typeCode);
		}

		public override bool IsIndexable {
			get { return true; }
		}

		private static string GetTypeString(SqlTypeCode sqlType) {
			if (sqlType == SqlTypeCode.DayToSecond)
				return "DAY TO SECOND";
			if (sqlType == SqlTypeCode.YearToMonth)
				return "YEAR TO MONTH";

			return "INTERVAL";
		}

		private static void AssertIsInterval(SqlTypeCode sqlType) {
			if (!IsIntervalType(sqlType))
				throw new ArgumentException(String.Format("SQL Type {0} is not a valid INTERVAL.", sqlType.ToString().ToUpperInvariant()));
		}

		public override bool IsCacheable(ISqlObject value) {
			return value is SqlNumber || value is SqlNull;
		}

		public override Type GetRuntimeType() {
			if (TypeCode == SqlTypeCode.DayToSecond)
				return typeof (TimeSpan);

			return base.GetRuntimeType();
		}

		public override Type GetObjectType() {
			if (TypeCode == SqlTypeCode.YearToMonth)
				return typeof (SqlYearToMonth);
			if (TypeCode == SqlTypeCode.DayToSecond)
				return typeof (SqlDayToSecond);

			return base.GetObjectType();
		}

		/// <inheritdoc/>
		public override bool IsComparable(SqlType type) {
			if (!(type is IntervalType))
				return false;

			// TODO: better check ...
			return TypeCode.Equals(type.TypeCode);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			if (TypeCode == SqlTypeCode.YearToMonth) {
				builder.Append("INTERVAL YEAR TO MONTH");
			} else if (TypeCode == SqlTypeCode.DayToSecond) {
				builder.Append("INTERVAL DAY TO SECOND");
			}
		}

		internal override int ColumnSizeOf(ISqlObject obj) {
			if (obj == null || obj.IsNull)
				return 1;

			if (obj is SqlDayToSecond)
				return 1 + 5;

			if (obj is SqlYearToMonth)
				return 1 + 4;

			return base.ColumnSizeOf(obj);
		}

		public override void SerializeObject(Stream stream, ISqlObject obj) {
			var writer = new BinaryWriter(stream);

			if (obj is SqlDayToSecond) {
				writer.Write((byte)1);

				var interval = (SqlDayToSecond) obj;

				if (interval.IsNull) {
					writer.Write((byte) 0);
				} else {
					writer.Write((byte) 1);

					var bytes = interval.ToByArray();

					writer.Write(bytes.Length);
					writer.Write(bytes);
				}
			} else if (obj is SqlYearToMonth) {
				writer.Write((byte)2);

				var interval = (SqlYearToMonth) obj;
				if (interval.IsNull) {
					writer.Write((byte) 0);
				} else {
					writer.Write((byte) 1);

					var months = interval.TotalMonths;
					writer.Write(months);
				}
			} else {
				throw new FormatException();
			}
		}

		public override ISqlObject DeserializeObject(Stream stream) {
			var reader = new BinaryReader(stream);

			var type = reader.ReadByte();

			if (type == 1) {
				var state = reader.ReadByte();
				if (state == 0)
					return SqlDayToSecond.Null;

				var length = reader.ReadInt32();
				var bytes = reader.ReadBytes(length);
				return new SqlDayToSecond(bytes);
			}
			if (type == 2) {
				var state = reader.ReadByte();
				if (state == 0)
					return SqlYearToMonth.Null;

				var months = reader.ReadInt32();
				return new SqlYearToMonth(months);
			}

			return base.DeserializeObject(stream);
		}

		internal static bool IsIntervalType(SqlTypeCode sqlType) {
			return sqlType == SqlTypeCode.YearToMonth  ||
			       sqlType == SqlTypeCode.DayToSecond;
		}
	}
}