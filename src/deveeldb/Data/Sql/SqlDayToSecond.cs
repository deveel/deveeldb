// 
//  Copyright 2010-2017 Deveel
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

namespace Deveel.Data.Sql {
	/// <summary>
	/// The value of a SQL <c>DAY TO SECOND</c>, representing a span of
	/// time expressed in days, hours, minutes, seconds and milliseconds
	/// </summary>
	public struct SqlDayToSecond : ISqlValue, IComparable<SqlDayToSecond>, IEquatable<SqlDayToSecond>, ISqlFormattable {
		private readonly TimeSpan value;

		public static readonly SqlDayToSecond Zero = new SqlDayToSecond(0, 0, 0, 0, 0);

		public SqlDayToSecond(int hours, int minutes, int seconds) 
			: this(0, hours, minutes, seconds) {
		}

		public SqlDayToSecond(int days, int hours, int minutes, int seconds) 
			: this(days, hours, minutes, seconds, 0) {
		}

		public SqlDayToSecond(int days, int hours, int minutes, int seconds, int milliseconds) {
			value = new TimeSpan(days, hours, minutes, seconds, milliseconds);
		}


		public SqlDayToSecond(byte[] bytes) {
			if (bytes == null)
				throw new ArgumentNullException("bytes");

			if (bytes.Length != 20)
				throw new ArgumentException("Invalid byte representation of DAY TO SECOND");

			var days = BitConverter.ToInt32(bytes, 0);
			var hours = BitConverter.ToInt32(bytes, 4);
			var minutes = BitConverter.ToInt32(bytes, 8);
			var seconds = BitConverter.ToInt32(bytes, 12);
			var millis = BitConverter.ToInt32(bytes, 16);

			value = new TimeSpan(days, hours, minutes, seconds, millis);
		}

		int IComparable.CompareTo(object obj) {
			return CompareTo((SqlDayToSecond) obj);
		}

		int IComparable<ISqlValue>.CompareTo(ISqlValue other) {
			return CompareTo((SqlDayToSecond) other);
		}

		public double TotalMilliseconds => value.TotalMilliseconds;

		public int Days => value.Days;

		public int Hours => value.Hours;

		public int Minutes => value.Minutes;

		public int Seconds => value.Seconds;

		public int Milliseconds => value.Milliseconds;

		bool ISqlValue.IsComparableTo(ISqlValue other) {
			return other is SqlDayToSecond;
		}

		public int CompareTo(SqlDayToSecond other) {
			return value.CompareTo(other.value);
		}

		public SqlDayToSecond Add(SqlDayToSecond interval) {
			var ts = new TimeSpan(interval.Days, interval.Hours, interval.Minutes, interval.Seconds, interval.Milliseconds);
			var result = value.Add(ts);
			return new SqlDayToSecond(result.Days, result.Hours, result.Minutes, result.Seconds, result.Milliseconds);
		}

		public SqlDayToSecond Subtract(SqlDayToSecond interval) {
			var ts = new TimeSpan(interval.Days, interval.Hours, interval.Minutes, interval.Seconds, interval.Milliseconds);
			var result = value.Subtract(ts);
			return new SqlDayToSecond(result.Days, result.Hours, result.Minutes, result.Seconds, result.Milliseconds);
		}

		public byte[] ToByArray() {
			var bytes = new byte[20];
			var days = BitConverter.GetBytes(value.Days);
			var hours = BitConverter.GetBytes(value.Hours);
			var minutes = BitConverter.GetBytes(value.Minutes);
			var seconds = BitConverter.GetBytes(value.Seconds);
			var millis = BitConverter.GetBytes(value.Milliseconds);

			Array.Copy(days, 0, bytes, 0, 4);
			Array.Copy(hours, 0, bytes, 4, 4);
			Array.Copy(minutes, 0, bytes, 8, 4);
			Array.Copy(seconds, 0, bytes, 12, 4);
			Array.Copy(millis, 0, bytes, 16, 4);
			return bytes;
		}

		public bool Equals(SqlDayToSecond other) {
			return value.Equals(other.value);
		}

		private SqlDayToSecond Negate() {
			var ts = value.Negate();
			return new SqlDayToSecond(ts.Days, ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
		}

		public override bool Equals(object obj) {
			return Equals((SqlDayToSecond) obj);
		}

		public override int GetHashCode() {
			return value.GetHashCode();
		}

		public static SqlDayToSecond operator +(SqlDayToSecond a, SqlDayToSecond b) {
			return a.Add(b);
		}

		public static SqlDayToSecond operator -(SqlDayToSecond a, SqlDayToSecond b) {
			return a.Subtract(b);
		}

		public static bool operator ==(SqlDayToSecond a, SqlDayToSecond b) {
			return a.CompareTo(b) == 0;
		}

		public static bool operator !=(SqlDayToSecond a, SqlDayToSecond b) {
			return !(a == b);
		}

		public static SqlDayToSecond operator -(SqlDayToSecond value) {
			return value.Negate();
		}

		public override string ToString() {
			return this.ToSqlString();
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			builder.Append(value.ToString("c"));
		}

		public static bool TryParse(string s, out SqlDayToSecond interval) {
			TimeSpan ts;
			if (!TimeSpan.TryParse(s, out ts)) {
				interval = new SqlDayToSecond();
				return false;
			}

			interval = new SqlDayToSecond(ts.Days, ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
			return true;
		}

		public static SqlDayToSecond Parse(string s) {
			SqlDayToSecond interval;
			if (!TryParse(s, out interval))
				throw new FormatException(String.Format("The input string '{0}' is not a valid interval.", s));

			return interval;
		}
	}
}