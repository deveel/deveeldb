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

		public override void SerializeObject(Stream stream, ISqlObject obj) {
			var writer = new BinaryWriter(stream);

			if (obj is SqlDayToSecond) {
				var interval = (SqlDayToSecond) obj;
				var bytes = interval.ToByArray();

				writer.Write((byte)1);
				writer.Write(bytes.Length);
				writer.Write(bytes);
			} else if (obj is SqlYearToMonth) {
				var interval = (SqlYearToMonth) obj;
				var months = interval.TotalMonths;

				writer.Write((byte)2);
				writer.Write(months);
			}

			throw new FormatException();
		}

		public override ISqlObject DeserializeObject(Stream stream) {
			var reader = new BinaryReader(stream);

			var type = reader.ReadByte();

			if (type == 1) {
				var length = reader.ReadInt32();
				var bytes = reader.ReadBytes(length);
				
				// TODO: implement the constructor from bytes
				throw new NotImplementedException();
			}
			if (type == 2) {
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