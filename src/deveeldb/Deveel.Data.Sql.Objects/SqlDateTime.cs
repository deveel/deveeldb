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
using System.Globalization;

namespace Deveel.Data.Sql.Objects {
	[Serializable]
	public struct SqlDateTime : ISqlObject, IEquatable<SqlDateTime>, IConvertible, IComparable<SqlDateTime> {
		private readonly DateTimeOffset? value;

		public static readonly SqlDateTime Null = new SqlDateTime(true);

		private const int DateSize = 7;
		private const int TimeStampSize = 11;
		private const int FullTimeStampSize = 13;

		public static readonly string[] SqlDateFormats = new[] {
			"yyyy-MM-dd",
			"yyyy MM dd"
		};

		public static readonly string[] SqlTimeStampFormats = new[] {
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

		public static readonly string[] SqlTimeFormats = new[] {
			"HH:mm:ss.fff z",
			"HH:mm:ss.fff zz",
			"HH:mm:ss.fff zzz",
			"HH:mm:ss.fff",
			"HH:mm:ss z",
			"HH:mm:ss zz",
			"HH:mm:ss zzz",
			"HH:mm:ss"
		};

		public static readonly SqlDateTime MaxDate = new SqlDateTime(9999, 12, 31, 23, 59, 59, 999);
		public static readonly SqlDateTime MinDate = new SqlDateTime(1, 1, 1, 0, 0, 0, 0);

		public SqlDateTime(int year, int month, int day)
			: this(year, month, day, 0, 0, 0, 0, SqlDayToSecond.Zero) {
		}

		public SqlDateTime(int year, int month, int day, int hour, int minute, int second, int millisecond)
			: this(year, month, day, hour, minute, second, millisecond, SqlDayToSecond.Zero) {
		}

		public SqlDateTime(int year, int month, int day, int hour, int minute, int second, int millisecond, SqlDayToSecond offset)
			: this() {
			if (year <= 0 || year > 9999)
				throw new ArgumentOutOfRangeException("year");
			if (month <= 0 || month > 12)
				throw new ArgumentOutOfRangeException("month");
			if (day <= 0 || day > 31)
				throw new ArgumentOutOfRangeException("day");

			if (hour < 0 || hour > 23)
				throw new ArgumentOutOfRangeException("hour");
			if (minute < 0 || minute > 59)
				throw new ArgumentOutOfRangeException("minute");
			if (second < 0 || second > 59)
				throw new ArgumentOutOfRangeException("second");
			if (millisecond < 0 || millisecond > 999)
				throw new ArgumentOutOfRangeException("millisecond");

			var tsOffset = new TimeSpan(0, offset.Hours, offset.Minutes, 0, 0);
			value = new DateTimeOffset(year, month, day, hour, minute, second, millisecond, tsOffset);
		}

		public SqlDateTime(long ticks)
			: this(ticks, SqlDayToSecond.Zero) {
		}

		public SqlDateTime(long ticks, SqlDayToSecond offset)
			: this() {
			var tsOffset = new TimeSpan(0, offset.Hours, offset.Minutes, 0);
			value = new DateTimeOffset(ticks, tsOffset);
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

            if (bytes.Length == DateSize) {
                millis = 0;
            } else {
                millis = bytes[7] << 24 | bytes[8] << 16 | bytes[9] <<  8 | bytes[10];
                if (bytes.Length == TimeStampSize) {
                    tzh = tzm = 0;
                } else {
                    tzh = bytes[11] - 20;
                    tzm = bytes[12] - 60;
                }
            }

			value = new DateTimeOffset(year, month, day, hour, minute, second, millis, new TimeSpan(0, tzh, tzm, 0, 0));
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

		/// <summary>
		/// Gets the offset between the date-time instance and the UTC time.
		/// </summary>
		public SqlDayToSecond Offset {
			get {
				AssertNotNull();
				return new SqlDayToSecond(0, value.Value.Offset.Hours, value.Value.Offset.Minutes, 0, 0);
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

			return value.Value.DateTime;
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
				var tsOffset = Offset;
				bytes[11] = (byte) tsOffset.Hours;
				bytes[12] = (byte) tsOffset.Minutes;
			}
			return bytes;
		}

		/// <summary>
		/// Adds the given interval of time to this date-time.
		/// </summary>
		/// <param name="interval">The interval of time to add.</param>
		/// <remarks>
		/// This method will return <see cref="Null"/> if either the given 
		/// <paramref name="interval"/> is <see cref="SqlDayToSecond.Null"/>
		/// or if this instance is equivalent to <c>NULL</c>.
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="SqlDateTime"/> that is the result of
		/// the addition to this date of the given interval of time.
		/// </returns>
		public SqlDateTime Add(SqlDayToSecond interval) {
			if (IsNull)
				return Null;
			if (interval.IsNull)
				return this;

			var result = value.Value.AddMilliseconds(interval.TotalMilliseconds);
			return new SqlDateTime(result.Ticks);
		}

		/// <summary>
		/// Subtracts a given interval of time from this date.
		/// </summary>
		/// <param name="interval">The interval to subtract from this date.</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlDateTime"/> that is the result
		/// of the subtraction of the given interval of time from this date value.
		/// </returns>
		/// <seealso cref="SqlDayToSecond"/>
		public SqlDateTime Subtract(SqlDayToSecond interval) {
			if (IsNull)
				return Null;
			if (interval.IsNull)
				return this;

			var result = value.Value.AddMilliseconds(-(interval.TotalMilliseconds));
			return new SqlDateTime(result.Ticks);
		}

		/// <summary>
		/// Adds the given months to this date.
		/// </summary>
		/// <param name="interval">The month-base interval of time to add.</param>
		/// <returns></returns>
		/// <seealso cref="SqlYearToMonth"/>
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

		public static SqlDateTime Parse(string s) {
			SqlDateTime date;
			if (!TryParse(s, out date))
				throw new FormatException(String.Format("Cannot convert string {0} to a valid SQL DATE", s));

			return date;
		}

		public static bool TryParse(string s, out SqlDateTime value) {
			value = new SqlDateTime();

			// We delegate parsing DATE and TIME strings to the .NET DateTime object...
			DateTimeOffset date;
			if (DateTimeOffset.TryParseExact(s, SqlDateFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out date)) {
				value = new SqlDateTime(date.Year, date.Month, date.Day, 0, 0, 0, 0, SqlDayToSecond.Zero);
				return true;
			}

			if (DateTimeOffset.TryParseExact(s, SqlTimeFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out date)) {
				var offset = new SqlDayToSecond(0, date.Offset.Hours, date.Offset.Minutes);
				value = new SqlDateTime(1, 1, 1, date.Hour, date.Minute, date.Second, date.Millisecond, offset);
				return true;
			}

			if (DateTimeOffset.TryParseExact(s, SqlTimeStampFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out date)) {
				var offset = new SqlDayToSecond(0, date.Offset.Hours, date.Offset.Minutes, 0);
				value = new SqlDateTime(date.Year, date.Month, date.Day, date.Hour, date.Minute, date.Second, date.Millisecond, offset);
				return true;
			}

			return false;
		}
	}
}