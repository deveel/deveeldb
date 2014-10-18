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
using System.Net.Configuration;

namespace Deveel.Data.Sql.Objects {
	[Serializable]
	public struct SqlDateTime : ISqlObject, IEquatable<SqlDateTime>, IConvertible, IComparable<SqlDateTime> {
		private readonly DateTime? value;

		public static readonly SqlDateTime Null = new SqlDateTime(true);

		private const int DateSize = 7;
		private const int TimeStampSize = 11;
		private const int FullTimeStampSize = 13;

		public SqlDateTime(int year, int month, int day)
			: this(year, month, day, 0, 0, 0, 0, DateTimeKind.Unspecified) {
		}

		public SqlDateTime(int year, int month, int day, int hour, int minute, int second, int millisecond)
			: this(year, month, day, hour, minute, second, millisecond, DateTimeKind.Unspecified) {
		}

		public SqlDateTime(int year, int month, int day, int hour, int minute, int second, int millisecond, DateTimeKind kind)
			: this() {
			value = new DateTime(year, month, day, hour, minute, second, millisecond, kind);
		}

		public SqlDateTime(long ticks)
			: this(ticks, DateTimeKind.Unspecified) {
		}

		public SqlDateTime(long ticks, DateTimeKind kind)
			: this() {
			value = new DateTime(ticks, kind);
		}

		private SqlDateTime(bool isNull)
			: this() {
			if (isNull)
				value = null;
		}

		public SqlDateTime(byte[] bytes)
			: this() {
			var year = ((bytes[0] - 100)*100) + (bytes[1] - 100);
			var month = (int) bytes[2];
			var day = (int) bytes[3];
			var hour = (int) bytes[4] - 1;
            var minute  = (int)bytes[5] - 1;
            var second  = (int)bytes[6] - 1;
			int millis;
			int tzh = 0, tzm = 0;

			var kind = DateTimeKind.Local;

            if (bytes.Length == DateSize) {
                millis = 0;
            }
            else {
                millis = bytes[7] << 24 | bytes[8] << 16 | bytes[9] <<  8 | bytes[10];
                if (bytes.Length == TimeStampSize) {
                    tzh = tzm = 0;
                } else {
                    tzh = bytes[11] - 20;
                    tzm = bytes[12] - 60;
                }
            }

			if (bytes.Length == FullTimeStampSize) {
				var utcValue = (new DateTime(year, month, day, hour, minute, second)
				.Add(new TimeSpan(tzh, tzm, 0)));

				year = utcValue.Year;
				month = utcValue.Month;
				day = utcValue.Day;
				hour = utcValue.Hour;
				minute = utcValue.Minute;
				kind = DateTimeKind.Utc;
			}

			value = new DateTime(year, month, day, hour, minute, second, millis, kind);
		}

		int IComparable.CompareTo(object obj) {
			return CompareTo((SqlDateTime) obj);
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			return CompareTo((SqlDateTime) other);
		}

		public bool IsNull {
			get { return value == null; }
		}

		private void AssertNotNull() {
			if (value == null)
				throw new InvalidOperationException();
		}

		public int Year {
			get {
				AssertNotNull();
				return value.Value.Year;
			}
		}

		public int Month {
			get {
				AssertNotNull();
				return value.Value.Month;
			}
		}

		public int Day {
			get {
				AssertNotNull();
				return value.Value.Day;
			}
		}

		public int Hour {
			get {
				AssertNotNull();
				return value.Value.Hour;
			}
		}

		public int Minute {
			get {
				AssertNotNull();
				return value.Value.Minute;
			}
		}

		public int Second {
			get {
				AssertNotNull();
				return value.Value.Second;
			}
		}

		public int Millisecond {
			get {
				AssertNotNull();
				return value.Value.Millisecond;
			}
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return other is SqlDateTime;
		}

		TypeCode IConvertible.GetTypeCode() {
			return TypeCode.DateTime;
		}

		bool IConvertible.ToBoolean(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		sbyte IConvertible.ToSByte(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		byte IConvertible.ToByte(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		short IConvertible.ToInt16(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		ushort IConvertible.ToUInt16(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		int IConvertible.ToInt32(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		uint IConvertible.ToUInt32(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		long IConvertible.ToInt64(IFormatProvider provider) {
			return ToInt64();
		}

		ulong IConvertible.ToUInt64(IFormatProvider provider) {
			return (ulong) ToInt64();
		}

		float IConvertible.ToSingle(IFormatProvider provider) {
			return ToInt64();
		}

		double IConvertible.ToDouble(IFormatProvider provider) {
			return ToInt64();
		}

		decimal IConvertible.ToDecimal(IFormatProvider provider) {
			throw new NotImplementedException();
		}

		DateTime IConvertible.ToDateTime(IFormatProvider provider) {
			if (value == null)
				throw new NullReferenceException();

			return value.Value;
		}

		string IConvertible.ToString(IFormatProvider provider) {
			return ToString();
		}

		object IConvertible.ToType(Type conversionType, IFormatProvider provider) {
			if (conversionType == typeof (long))
				return ToInt64();
			if (conversionType == typeof (float))
				return (float) ToInt64();
			if (conversionType == typeof (double))
				return (double) ToInt64();

			if (conversionType == typeof (string))
				return ToString();

			if (conversionType == typeof (byte[]))
				return ToByteArray();

			throw new InvalidCastException();
		}

		public bool Equals(SqlDateTime other) {
			if (IsNull && other.IsNull)
				return true;

			return value.Equals(other.value);
		}

		public override bool Equals(object obj) {
			return Equals((SqlDateTime) obj);
		}

		public override int GetHashCode() {
			return value == null ? 0 : value.GetHashCode();
		}

		public int CompareTo(SqlDateTime other) {
			if (!value.HasValue && !other.value.HasValue)
				return 0;
			if (!value.HasValue)
				return 1;
			if (!other.value.HasValue)
				return -1;

			return value.Value.CompareTo(other.value.Value);
		}

		public long ToInt64() {
			AssertNotNull();
			return value.Value.Ticks;
		}

		public byte[] ToByteArray() {
			return ToByteArray(false);
		}

		public byte[] ToByteArray(bool timeZone) {
			if (IsNull)
				return new byte[11];

			var bytes = new byte[11];
			bytes[0] = (byte)((Year / 100) + 100);
            bytes[1] = (byte)((Year % 100) + 100);
            bytes[2] = (byte)(Month);
            bytes[3] = (byte)(Day);
            bytes[4] = (byte)(Hour   + 1);
            bytes[5] = (byte)(Minute + 1);
            bytes[6] = (byte)(Second + 1);
            bytes[7] = (byte)((Millisecond >> 24));
            bytes[8] = (byte)((Millisecond >> 16) & 0xff);
            bytes[9] = (byte)((Millisecond >>  8) & 0xff);
            bytes[10]= (byte)(Millisecond & 0xff);
			if (timeZone) {
				var tsOffset = TimeZone.CurrentTimeZone.GetUtcOffset(value.Value);
				bytes[11] = (byte) tsOffset.Hours;
				bytes[12] = (byte) tsOffset.Minutes;
			}
			return bytes;
		}

		public SqlDateTime Add(SqlDayToSecond interval) {
			if (IsNull)
				return Null;
			if (interval.IsNull)
				return this;

			var result = value.Value.AddMilliseconds(interval.TotalMilliseconds);
			return new SqlDateTime(result.Ticks);
		}

		public SqlDateTime Subtract(SqlDayToSecond interval) {
			if (IsNull)
				return Null;
			if (interval.IsNull)
				return this;

			var result = value.Value.AddMilliseconds(-(interval.TotalMilliseconds));
			return new SqlDateTime(result.Ticks);
		}

		public SqlDateTime Add(SqlYearToMonth interval) {
			if (IsNull)
				return Null;
			if (interval.IsNull)
				return this;

			var result = value.Value.AddMonths(interval.TotalMonths);
			return new SqlDateTime(result.Ticks);
		}

		public SqlDateTime Subtract(SqlYearToMonth interval) {
			if (IsNull)
				return Null;
			if (interval.IsNull)
				return this;

			var result = value.Value.AddMonths(-interval.TotalMonths);
			return new SqlDateTime(result.Ticks);			
		}

		public static bool operator ==(SqlDateTime a, SqlDateTime b) {
			return a.Equals(b);
		}

		public static bool operator !=(SqlDateTime a, SqlDateTime b) {
			return !(a == b);
		}

		public static bool operator >(SqlDateTime a, SqlDateTime b) {
			return a.CompareTo(b) < 0;
		}

		public static bool operator <(SqlDateTime a, SqlDateTime b) {
			return a.CompareTo(b) > 0;
		}

		public static bool operator >=(SqlDateTime a, SqlDateTime b) {
			var i = a.CompareTo(b);
			return i == 0 || i < 0;
		}

		public static bool operator <=(SqlDateTime a, SqlDateTime b) {
			var i = a.CompareTo(b);
			return i == 0 || i > 0;
		}

		public static SqlDateTime operator +(SqlDateTime a, SqlDayToSecond b) {
			return a.Add(b);
		}

		public static SqlDateTime operator -(SqlDateTime a, SqlDayToSecond b) {
			return a.Subtract(b);
		}

		public static SqlDateTime operator +(SqlDateTime a, SqlYearToMonth b) {
			return a.Add(b);
		}

		public static SqlDateTime operator -(SqlDateTime a, SqlYearToMonth b) {
			return a.Subtract(b);
		}
	}
}