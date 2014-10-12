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

using Deveel.Data.Types;

namespace Deveel.Data {
	[Serializable]
	public sealed class DateObject : DataObject, IComparable, IComparable<DateObject>, IEquatable<DateObject> {
		private readonly DateTime? value;

		public static readonly DateObject Null = new DateObject(PrimitiveTypes.Date(), null);
		public static readonly DateObject UtcNow = new DateObject(PrimitiveTypes.Date(SqlTypeCode.TimeStamp), DateTime.UtcNow);
		public static readonly DateObject Today = new DateObject(PrimitiveTypes.Date(SqlTypeCode.Date), DateTime.Today);

		private DateObject(DateType type, DateTime? value)
			: base(type) {
			this.value = value;
		}

		public DateObject(int year, int month, int day)
			: this(PrimitiveTypes.Date(SqlTypeCode.Date), new DateTime(year, month, day)) {
		}

		public DateObject(int year, int month, int day, int hour, int minute, int second)
			: this(year, month, day, hour, minute, second, 0) {
		}

		public DateObject(int year, int month, int day, int hour, int minute, int second, int millisecond)
			: this(PrimitiveTypes.Date(SqlTypeCode.TimeStamp), new DateTime(year, month, day, hour, minute, second, millisecond)) {
		}

		public override bool IsNull {
			get { return value == null; }
		}

		public int Year {
			get { return value == null ? 0 : value.Value.Year; }
		}

		public int Month {
			get { return value == null ? 0 : value.Value.Month; }
		}

		public int Day {
			get { return value == null ? 0 : value.Value.Day; }
		}

		public int Hour {
			get { return value == null ? 0 : value.Value.Hour; }
		}

		public int Minute {
			get { return value == null ? 0 : value.Value.Minute; }
		}

		public int Second {
			get { return value == null ? 0 : value.Value.Second; }
		}

		public int Millisecond {
			get { return value == null ? 0 : value.Value.Millisecond; }
		}

		public override bool Equals(object obj) {
			var other = obj as DateObject;
			if (other == null)
				return false;

			return Equals(other);
		}

		public override int GetHashCode() {
			return value == null ? 0 : value.Value.GetHashCode();
		}

		int IComparable.CompareTo(object obj) {
			if (!(obj is DateObject))
				throw new ArgumentException();

			return CompareTo((DateObject) obj);
		}

		public int CompareTo(DateObject other) {
			if (other == null)
				return -1;

			if (value == null && other.value != null)
				return 1;
			if (value == null && other.value == null)
				return 0;

			if (value == null)
				return 1;

			return value.Value.CompareTo(other.value);
		}

		public bool Equals(DateObject other) {
			if (other == null)
				return false;

			if (value == null && other.value == null)
				return true;
			if (value == null)
				return false;

			return value.Value.Equals(other.value);
		}

		// TODO: Make it formattable..
		public override string ToString() {
			if (value == null)
				return "NULL";

			if (Type.SqlType == SqlTypeCode.Time)
				return value.Value.ToString(DateType.TimeFormatSql[0]);
			if (Type.SqlType == SqlTypeCode.Date)
				return value.Value.ToString(DateType.DateFormatSql[0]);
			if (Type.SqlType == SqlTypeCode.TimeStamp)
				return value.Value.ToString(DateType.TsFormatSql[0]);

			return base.ToString();
		}

		public static bool TryParse(string s, out DateObject value) {
			if (String.IsNullOrEmpty(s))
				throw new ArgumentNullException("s");

			value = null;

			DateTime date;
			if (DateTime.TryParseExact(s, DateType.TsFormatSql, CultureInfo.InvariantCulture, DateTimeStyles.None, out date)) {
				value = new DateObject(PrimitiveTypes.Date(SqlTypeCode.TimeStamp), date);
				return true;
			}

			if (DateTime.TryParseExact(s, DateType.DateFormatSql, CultureInfo.InvariantCulture, DateTimeStyles.None, out date)) {
				value = new DateObject(PrimitiveTypes.Date(SqlTypeCode.Date), date);
				return true;
			}

			if (DateTime.TryParseExact(s, DateType.TimeFormatSql, CultureInfo.InvariantCulture, DateTimeStyles.None, out date)) {
				value = new DateObject(PrimitiveTypes.Date(SqlTypeCode.Time), date);
				return true;
			}

			return false;
		}

		public static DateObject Parse(string s) {
			DateObject value;
			if (!TryParse(s, out value))
				throw new FormatException();

			return value;
		}

		public static bool operator ==(DateObject a, DateObject b) {
			if (Equals(a, null) && Equals(b, null))
				return true;
			if (Equals(a, null))
				return false;

			return a.Equals(b);
		}

		public static bool operator !=(DateObject a, DateObject b) {
			return !(a == b);
		}

		public static bool operator >(DateObject a, DateObject b) {
			return a.CompareTo(b) < 0;
		}

		public static bool operator <(DateObject a, DateObject b) {
			return a.CompareTo(b) > 0;
		}

		public static bool operator >=(DateObject a, DateObject b) {
			var i = a.CompareTo(b);
			return i == 0 || i < 0;
		}

		public static bool operator <=(DateObject a, DateObject b) {
			var i = a.CompareTo(b);
			return i == 0 || i > 0;
		}

		public static implicit operator DateTime(DateObject obj) {
			if (obj.value == null)
				throw new NullReferenceException();

			return obj.value.Value;
		}

		public static implicit operator DateObject(DateTime d) {
			// TODO: check if we have a TS or a DATE
			return new DateObject(PrimitiveTypes.Date(SqlTypeCode.TimeStamp), d);
		}
	}
}
