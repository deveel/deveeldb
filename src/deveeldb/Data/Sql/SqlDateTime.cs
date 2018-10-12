// 
//  Copyright 2010-2018 Deveel
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
using System.Collections.Generic;
using System.Globalization;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Represents a SQL date and time with or without timezone
	/// </summary>
	/// <remarks>
	/// <para>
	/// This value has a variable handling of precision and it's not
	/// by itself bound to certain limits, that are instead defined
	/// by <see cref="SqlDateTimeType"/>.
	/// </para>
	/// </remarks>
	public struct SqlDateTime : ISqlValue, IEquatable<SqlDateTime>, IComparable<SqlDateTime>, IFormattable, IConvertible {
		private readonly DateTimeOffset value;

		private const int DateSize = 7;
		private const int TimeStampSize = 11;
		private const int FullTimeStampSize = 13;

		/// <summary>
		/// The list of valid formats handled during a parse of a date (excluding time)
		/// </summary>
		/// <seealso cref="Parse"/>
		/// <seealso cref="TryParse"/>
		/// <seealso cref="TryParseDate"/>
		public static readonly string[] SqlDateFormats = new[] {
			"yyyy-MM-dd",
			"yyyy MM dd"
		};

		/// <summary>
		/// The list of valid formats handled during a parse of a full timestamp
		/// </summary>
		/// <seealso cref="Parse"/>
		/// <seealso cref="TryParse"/>
		/// <seealso cref="TryParseTimeStamp(string,out SqlDateTime)"/>
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

		/// <summary>
		/// The list of valid formats handled during a parse of times (excluding the date part)
		/// </summary>
		/// <seealso cref="Parse"/>
		/// <seealso cref="TryParse"/>
		/// <seealso cref="TryParseTime"/>
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

		/// <summary>
		/// The format that a SQL Time is represented as string by default
		/// </summary>
		/// <seealso cref="ToString()"/>
		/// <seealso cref="ToString(string)"/>
		public const string TimeStringFormat = "HH:mm:ss.fff zzz";

		/// <summary>
		/// The format that a full SQL TimeStamp is represented as string by default
		/// </summary>
		/// <seealso cref="ToString()"/>
		/// <seealso cref="ToString(string)"/>
		public const string TimeStampStringFormat = "yyyy-MM-ddTHH:mm:ss.fff zzz";

		/// <summary>
		/// The format that a SQL Date is represented as string by default
		/// </summary>
		/// <seealso cref="ToString()"/>
		/// <seealso cref="ToString(string)"/>
		public const string DateStringFormat = "yyyy-MM-dd";

		/// <summary>
		/// The maximum date-time that can be handled by system
		/// </summary>
		public static readonly SqlDateTime MaxDate = new SqlDateTime(9999, 12, 31, 23, 59, 59, 999);

		/// <summary>
		/// The minimum date-time that can be handled by the system
		/// </summary>
		public static readonly SqlDateTime MinDate = new SqlDateTime(1, 1, 1, 0, 0, 0, 0);

		private static readonly Dictionary<string, string> tsAbbreviations;

		/// <summary>
		/// Constructs a new SQL date
		/// </summary>
		/// <param name="year">The year part of the date (must between 0 and 9999)</param>
		/// <param name="month">The month part of the date (must be between 1 and 12)</param>
		/// <param name="day">The day part of the date (must be between 1 and 31)</param>
		/// <exception cref="ArgumentOutOfRangeException">If either one of the arguments
		/// is not in a valid range of values</exception>
		public SqlDateTime(int year, int month, int day)
			: this(year, month, day, 0, 0, 0, 0, SqlDayToSecond.Zero) {
		}

		/// <summary>
		/// Constructs a new UTC SQL date-time
		/// </summary>
		/// <param name="year">The year part of the date (must between 0 and 9999)</param>
		/// <param name="month">The month part of the date (must be between 1 and 12)</param>
		/// <param name="day">The day part of the date (must be between 1 and 31)</param>
		/// <param name="hour">The hour part of the time (must be between 0 and 23)</param>
		/// <param name="minute">The minute part of the time (must be between 0 and 59)</param>
		/// <param name="second">The second part of the time (must be between 0 and 59)</param>
		/// <param name="millisecond">The milliseconds part of the time (must be between 0 and 999)</param>
		/// <exception cref="ArgumentOutOfRangeException">If either one of the arguments
		/// is not in a valid range of values</exception>
		public SqlDateTime(int year, int month, int day, int hour, int minute, int second, int millisecond)
			: this(year, month, day, hour, minute, second, millisecond, SqlDayToSecond.Zero) {
		}

		/// <summary>
		/// Constructs a new SQL date-time with full information
		/// </summary>
		/// <param name="year">The year part of the date (must between 0 and 9999)</param>
		/// <param name="month">The month part of the date (must be between 1 and 12)</param>
		/// <param name="day">The day part of the date (must be between 1 and 31)</param>
		/// <param name="hour">The hour part of the time (must be between 0 and 23)</param>
		/// <param name="minute">The minute part of the time (must be between 0 and 59)</param>
		/// <param name="second">The second part of the time (must be between 0 and 59)</param>
		/// <param name="millisecond">The milliseconds part of the time (must be between 0 and 999)</param>
		/// <param name="offset">The offset of the date from the UTC</param>
		/// <exception cref="ArgumentOutOfRangeException">If either one of the arguments
		/// is not in a valid range of values</exception>
		public SqlDateTime(int year, int month, int day, int hour, int minute, int second, int millisecond,
			SqlDayToSecond offset)
			: this() {
			if (year <= 0 || year > 9999)
				throw new ArgumentOutOfRangeException(nameof(year));
			if (month <= 0 || month > 12)
				throw new ArgumentOutOfRangeException(nameof(month));
			if (day <= 0 || day > 31)
				throw new ArgumentOutOfRangeException(nameof(day));

			if (hour < 0 || hour > 23)
				throw new ArgumentOutOfRangeException(nameof(hour));
			if (minute < 0 || minute > 59)
				throw new ArgumentOutOfRangeException(nameof(minute));
			if (second < 0 || second > 59)
				throw new ArgumentOutOfRangeException(nameof(second));
			if (millisecond < 0 || millisecond > 999)
				throw new ArgumentOutOfRangeException(nameof(millisecond));

			var tsOffset = new TimeSpan(0, offset.Hours, offset.Minutes, 0, 0);
			value = new DateTimeOffset(year, month, day, hour, minute, second, millisecond, tsOffset);
		}

		/// <summary>
		/// Constructs a new SQL date-time from the given ticks
		/// </summary>
		/// <param name="ticks"></param>
		public SqlDateTime(long ticks)
			: this(ticks, SqlDayToSecond.Zero) {
		}

		public SqlDateTime(long ticks, SqlDayToSecond offset)
			: this() {
			var tsOffset = new TimeSpan(0, offset.Hours, offset.Minutes, 0);
			value = new DateTimeOffset(ticks, tsOffset);
		}

		/// <summary>
		/// Constructs a new SQL date-time from its binary representation
		/// </summary>
		/// <param name="bytes">The binary representation of the date-time to construct</param>
		/// <remarks>
		/// <para>
		/// The valid length of the provided array can be of 7-bytes for dates (no time information),
		/// 11-bytes for timestamps without UTC-offset or 13-bytes for a full form that
		/// includes the offset from UTC.
		/// </para>
		/// </remarks>
		public SqlDateTime(byte[] bytes)
			: this() {
			var year = ((bytes[0] - 100) * 100) + (bytes[1] - 100);
			var month = (int) bytes[2];
			var day = (int) bytes[3];
			var hour = (int) bytes[4] - 1;
			var minute = (int) bytes[5] - 1;
			var second = (int) bytes[6] - 1;
			int millis;
			int tzh = 0, tzm = 0;

			if (bytes.Length == DateSize) {
				millis = 0;
			} else {
				millis = bytes[7] << 24 | bytes[8] << 16 | bytes[9] << 8 | bytes[10];
				if (bytes.Length == TimeStampSize) {
					tzh = tzm = 0;
				} else {
					tzh = bytes[11] - 20;
					tzm = bytes[12] - 60;
				}
			}

			value = new DateTimeOffset(year, month, day, hour, minute, second, millis, new TimeSpan(0, tzh, tzm, 0, 0));
		}

		static SqlDateTime() {
			tsAbbreviations = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) {
				{"ACDT", "Australian Central Daylight Savings Time"},
				{"ACST", "Australian Central Standard Time"},
				{"ACT", "Acre Time"},
				{"ADT", "Atlantic Daylight Time"},
				{"AEDT", "Australian Eastern Daylight Savings Time"},
				{"AEST", "Australian Eastern Standard Time"},
				{"AFT", "Afghanistan Time"},
				{"CET", "Central European Standard Time"},
				{"EST", "Eastern Standard Time"},
				{"PST", "Pacific Standard Time"},
				{"GMT", "Greenwich Mean Time"}

				// TODO: Continue!
			};
		}

		/// <summary>
		/// Gets the year part of the date
		/// </summary>
		public int Year => value.Year;

		/// <summary>
		/// Gets the month part of the date
		/// </summary>
		public int Month => value.Month;

		/// <summary>
		/// Gets the day part of the date
		/// </summary>
		public int Day => value.Day;

		public int Hour => value.Hour;

		public int Minute => value.Minute;

		public int Second => value.Second;

		public int Millisecond => value.Millisecond;

		public long Ticks => value.Ticks;

		/// <summary>
		/// Gets the offset between the date-time instance and the UTC time.
		/// </summary>
		public SqlDayToSecond Offset => new SqlDayToSecond(0, value.Offset.Hours, value.Offset.Minutes, 0, 0);

		public SqlDateTime DatePart => new SqlDateTime(Year, Month, Day);

		public SqlDateTime TimePart => new SqlDateTime(1, 1, 1, Hour, Minute, Second, Millisecond);

		public static SqlDateTime Now {
			get {
				var date = DateTimeOffset.Now;
				var offset = new SqlDayToSecond(date.Offset.Days, date.Offset.Hours, date.Offset.Minutes, date.Minute);
				return new SqlDateTime(date.Year, date.Month, date.Day, date.Hour, date.Minute, date.Second, date.Millisecond,
					offset);
			}
		}

		public DayOfWeek DayOfWeek => value.DayOfWeek;

		bool ISqlValue.IsComparableTo(ISqlValue other) {
			return other is SqlDateTime;
		}

		int IComparable.CompareTo(object obj) {
			return CompareTo((SqlDateTime) obj);
		}

		int IComparable<ISqlValue>.CompareTo(ISqlValue other) {
			if (!(other is SqlDateTime))
				throw new ArgumentException();

			return CompareTo((SqlDateTime) other);
		}

		public int CompareTo(SqlDateTime other) {
			return value.CompareTo(other.value);
		}

		TypeCode IConvertible.GetTypeCode() {
			return TypeCode.DateTime;
		}

		#region Comparable

		bool IConvertible.ToBoolean(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		sbyte IConvertible.ToSByte(IFormatProvider provider) {
			throw new InvalidCastException();
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
			throw new InvalidCastException();
		}

		double IConvertible.ToDouble(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		decimal IConvertible.ToDecimal(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		DateTime IConvertible.ToDateTime(IFormatProvider provider) {
			if (value == null)
				throw new NullReferenceException();

			return value.DateTime;
		}

		string IConvertible.ToString(IFormatProvider provider) {
			return ToString();
		}

		object IConvertible.ToType(Type conversionType, IFormatProvider provider) {
			if (conversionType == typeof(byte[]))
				return ToByteArray();
			if (conversionType == typeof(DateTimeOffset))
				return ToDateTimeOffset();

			throw new InvalidCastException();
		}

		private long ToInt64() {
			return value.Ticks;
		}

		#endregion

		public bool Equals(SqlDateTime other) {
			return value.Equals(other.value);
		}

		public override bool Equals(object obj) {
			if (!(obj is SqlDateTime))
				return false;

			return Equals((SqlDateTime) obj);
		}

		public override int GetHashCode() {
			return value.GetHashCode();
		}

		/// <summary>
		/// Adds the given interval of time to this date-time.
		/// </summary>
		/// <param name="interval">The interval of time to add.</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlDateTime"/> that is the result of
		/// the addition to this date of the given interval of time.
		/// </returns>
		public SqlDateTime Add(SqlDayToSecond interval) {
			var result = value.AddMilliseconds(interval.TotalMilliseconds);
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
			var result = value.AddMilliseconds(-(interval.TotalMilliseconds));
			return new SqlDateTime(result.Ticks);
		}

		/// <summary>
		/// Adds the given months to this date.
		/// </summary>
		/// <param name="interval">The month-base interval of time to add.</param>
		/// <returns></returns>
		/// <seealso cref="SqlYearToMonth"/>
		public SqlDateTime Add(SqlYearToMonth interval) {
			var result = value.AddMonths(interval.TotalMonths);
			return new SqlDateTime(result.Ticks);
		}

		public SqlDateTime AddDays(int days) {
			return Add(new SqlDayToSecond(days, 0, 0, 0));
		}

		public SqlDateTime Subtract(SqlYearToMonth interval) {
			var result = value.AddMonths(-interval.TotalMonths);
			return new SqlDateTime(result.Ticks);
		}

		public SqlDateTime GetNextDateForDay(DayOfWeek desiredDay) {
			// Given a date and day of week,
			// find the next date whose day of the week equals the specified day of the week.
			return AddDays(DaysToAdd(DayOfWeek, desiredDay));
		}

		private static int DaysToAdd(DayOfWeek current, DayOfWeek desired) {
			// f( c, d ) = g( c, d ) mod 7, g( c, d ) > 7
			//           = g( c, d ), g( c, d ) < = 7
			//   where 0 <= c < 7 and 0 <= d < 7

			int c = (int) current;
			int d = (int) desired;
			int n = (7 - c + d);

			return (n > 7) ? n % 7 : n;
		}

		#region Operators

		public static bool operator ==(SqlDateTime a, SqlDateTime b) {
			return a.Equals(b);
		}

		public static bool operator !=(SqlDateTime a, SqlDateTime b) {
			return !(a == b);
		}

		public static bool operator >(SqlDateTime a, SqlDateTime b) {
			return a.CompareTo(b) > 0;
		}

		public static bool operator <(SqlDateTime a, SqlDateTime b) {
			return a.CompareTo(b) < 0;
		}

		public static bool operator >=(SqlDateTime a, SqlDateTime b) {
			return a.CompareTo(b) >= 0;
		}

		public static bool operator <=(SqlDateTime a, SqlDateTime b) {
			return a.CompareTo(b) <= 0;
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

		#endregion

		#region Parse

		public static SqlDateTime Parse(string s) {
			SqlDateTime date;
			if (!TryParse(s, out date))
				throw new FormatException(String.Format("Cannot convert string {0} to a valid SQL DATE", s));

			return date;
		}

		public static bool TryParse(string s, out SqlDateTime value) {
			// We delegate parsing DATE and TIME strings to the .NET DateTime object...
			if (TryParseDate(s, out value))
				return true;

			if (TryParseTime(s, out value))
				return true;

			if (TryParseTimeStamp(s, out value))
				return true;

			value = new SqlDateTime();
			return false;
		}

		public static bool TryParseDate(string s, out SqlDateTime value) {
			value = new SqlDateTime();

			// We delegate parsing DATE and TIME strings to the .NET DateTime object...
			DateTimeOffset date;
			if (DateTimeOffset.TryParseExact(s, SqlDateFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out date)) {
				value = new SqlDateTime(date.Year, date.Month, date.Day, 0, 0, 0, 0, SqlDayToSecond.Zero);
				return true;
			}

			return false;
		}

		public static bool TryParseTime(string s, out SqlDateTime value) {
			value = new SqlDateTime();

			// We delegate parsing DATE and TIME strings to the .NET DateTime object...
			DateTimeOffset date;
			if (DateTimeOffset.TryParseExact(s, SqlTimeFormats, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal,
				out date)) {
				var offset = new SqlDayToSecond(date.Offset.Hours, date.Offset.Minutes, 0);
				value = new SqlDateTime(1, 1, 1, date.Hour, date.Minute, date.Second, date.Millisecond, offset);
				return true;
			}

			return false;
		}

		public static bool TryParseTimeStamp(string s, out SqlDateTime value) {
			return TryParseTimeStamp(s, (string)null, out value);
		}

		public static bool TryParseTimeStamp(string s, string timeZone, out SqlDateTime value) {
			TimeZoneInfo timeZoneInfo = null;

			if (!String.IsNullOrEmpty(timeZone)) {
				string norm;
				if (tsAbbreviations.TryGetValue(timeZone, out norm))
					timeZone = norm;

				timeZoneInfo = TimeZoneInfo.FindSystemTimeZoneById(timeZone);
				if (timeZoneInfo == null)
					throw new InvalidTimeZoneException(String.Format("Time-zone ID '{0}' is invalid", timeZone));
			}

			return TryParseTimeStamp(s, timeZoneInfo, out value);
		}

		public static bool TryParseTimeStamp(string s, TimeZoneInfo timeZone, out SqlDateTime value) {
			value = new SqlDateTime();

			// We delegate parsing DATE and TIME strings to the .NET DateTime object...
			DateTimeOffset date;
			if (DateTimeOffset.TryParseExact(s, SqlTimeStampFormats, CultureInfo.InvariantCulture,
				DateTimeStyles.AssumeUniversal, out date)) {
				SqlDayToSecond offset;
				if (timeZone != null) {
					var utcOffset = timeZone.GetUtcOffset(date);
					offset = new SqlDayToSecond(utcOffset.Hours, utcOffset.Minutes, 0);
				} else {
					offset = new SqlDayToSecond(date.Offset.Hours, date.Offset.Minutes, 0);
				}

				value = new SqlDateTime(date.Year, date.Month, date.Day, date.Hour, date.Minute, date.Second, date.Millisecond,
					offset);
				return true;
			}

			return false;
		}

		#endregion

		#region Explicit Operators

		public static explicit operator SqlDateTime(DateTimeOffset? a) {
			return (SqlDateTime) a.Value;
		}

		public static explicit operator SqlDateTime(DateTimeOffset a) {
			var date = a;
			var offset = new SqlDayToSecond(date.Offset.Days, date.Offset.Hours, date.Offset.Minutes, date.Offset.Seconds);
			return new SqlDateTime(date.Year, date.Month, date.Day, date.Hour, date.Minute, date.Second, date.Millisecond,
				offset);
		}

		public static explicit operator DateTimeOffset(SqlDateTime a) {
			var offset = new TimeSpan(a.Offset.Hours, a.Offset.Minutes, a.Offset.Seconds);
			return new DateTimeOffset(a.Year, a.Month, a.Day, a.Hour, a.Minute, a.Second, a.Millisecond, offset);
		}

		#endregion

		public byte[] ToByteArray() {
			return ToByteArray(false);
		}

		public byte[] ToByteArray(bool timeZone) {
			var size = timeZone ? 13 : 11;

			var bytes = new byte[size];
			bytes[0] = (byte) ((Year / 100) + 100);
			bytes[1] = (byte) ((Year % 100) + 100);
			bytes[2] = (byte) (Month);
			bytes[3] = (byte) (Day);
			bytes[4] = (byte) (Hour + 1);
			bytes[5] = (byte) (Minute + 1);
			bytes[6] = (byte) (Second + 1);
			bytes[7] = (byte) ((Millisecond >> 24));
			bytes[8] = (byte) ((Millisecond >> 16) & 0xff);
			bytes[9] = (byte) ((Millisecond >> 8) & 0xff);
			bytes[10] = (byte) (Millisecond & 0xff);
			if (timeZone) {
				var tsOffset = Offset;
				bytes[11] = (byte) (tsOffset.Hours + 20);
				bytes[12] = (byte) (tsOffset.Minutes + 60);
			}
			return bytes;
		}

		public SqlDateTime ToUtc() {
			var utc = value.ToUniversalTime();
			var offset = new SqlDayToSecond(utc.Offset.Days, utc.Offset.Hours, utc.Offset.Minutes);
			return new SqlDateTime(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, utc.Second, utc.Millisecond, offset);
		}

		public SqlString ToDateString() {
			var s = value.ToString(DateStringFormat, CultureInfo.InvariantCulture);
			return new SqlString(s);
		}

		public SqlString ToTimeString() {
			var s = value.ToString(TimeStringFormat, CultureInfo.InvariantCulture);
			return new SqlString(s);
		}

		public SqlString ToTimeStampString() {
			var s = value.ToString(TimeStampStringFormat, CultureInfo.InvariantCulture);
			return new SqlString(s);
		}

		public override string ToString() {
			return ToTimeStampString().ToString();
		}

		public string ToString(string format, IFormatProvider formatProvider) {
			return value.ToString(format, formatProvider);
		}

		public string ToString(string format) {
			return ToString(format, CultureInfo.InvariantCulture);
		}

		public DateTime ToDateTime() {
			return value.DateTime;
		}

		public DateTimeOffset ToDateTimeOffset() {
			return value;
		}

		public SqlDateTime AtTimeZone(TimeZoneInfo timeZone) {
			var utcOffset = timeZone.GetUtcOffset(value.LocalDateTime);
			return (SqlDateTime) value.ToOffset(utcOffset);
		}

		public SqlDateTime AtTimeZone(string timeZone) {
			string norm;
			if (tsAbbreviations.TryGetValue(timeZone, out norm))
				timeZone = norm;

			var timeZoneInfo = TimeZoneInfo.FindSystemTimeZoneById(timeZone);
			if (timeZoneInfo == null)
				throw new ArgumentException(String.Format("Time-zone ID '{0}' is invalid", timeZone));

			return AtTimeZone(timeZoneInfo);
		}
	}
}