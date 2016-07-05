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

namespace Deveel.Data.Sql.Objects {
	[Serializable]
	public struct SqlDayToSecond : ISqlObject, IComparable<SqlDayToSecond>, IEquatable<SqlDayToSecond> {
		private readonly TimeSpan? value;

		public static readonly SqlDayToSecond Null = new SqlDayToSecond(true);
		public static readonly SqlDayToSecond Zero = new SqlDayToSecond(0, 0, 0, 0, 0);

		private SqlDayToSecond(bool isNull) {
			value = null;
		}

		public SqlDayToSecond(int hours, int minutes, int seconds) 
			: this(0, hours, minutes, seconds) {
		}

		public SqlDayToSecond(int days, int hours, int minutes, int seconds) 
			: this(days, hours, minutes, seconds, 0) {
		}

		public SqlDayToSecond(int days, int hours, int minutes, int seconds, int milliseconds) {
			value = new TimeSpan(days, hours, minutes, seconds, milliseconds);
		}

		int IComparable.CompareTo(object obj) {
			return CompareTo((SqlDayToSecond) obj);
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			return CompareTo((SqlDayToSecond) other);
		}

		public bool IsNull {
			get { return value == null; }
		}

		private void AssertNotNull() {
			if (value == null)
				throw new NullReferenceException();
		}

		public double TotalMilliseconds {
			get {
				AssertNotNull();
				return value.Value.TotalMilliseconds;
			}
		}

		public int Days {
			get {
				AssertNotNull();
				return value.Value.Days;
			}
		}

		public int Hours {
			get {
				AssertNotNull();
				return value.Value.Hours;
			}
		}

		public int Minutes {
			get {
				AssertNotNull();
				return value.Value.Minutes;
			}
		}

		public int Seconds {
			get {
				AssertNotNull();
				return value.Value.Seconds;
			}
		}

		public int Milliseconds {
			get {
				AssertNotNull();
				return value.Value.Milliseconds;
			}
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return other is SqlDayToSecond;
		}

		public int CompareTo(SqlDayToSecond other) {
			if (IsNull && other.IsNull)
				return 0;
			if (!IsNull && other.IsNull)
				return -1;
			if (IsNull && !other.IsNull)
				return 1;

			return value.Value.CompareTo(other.value.Value);
		}

		public SqlDayToSecond Add(SqlDayToSecond interval) {
			if (IsNull)
				return interval;
			if (interval.IsNull)
				return Null;

			var ts = new TimeSpan(interval.Days, interval.Hours, interval.Minutes, interval.Seconds, interval.Milliseconds);
			var result = value.Value.Add(ts);
			return new SqlDayToSecond(result.Days, result.Hours, result.Minutes, result.Seconds, result.Milliseconds);
		}

		public SqlDayToSecond Subtract(SqlDayToSecond interval) {
			if (IsNull)
				return interval;
			if (interval.IsNull)
				return this;

			var ts = new TimeSpan(interval.Days, interval.Hours, interval.Minutes, interval.Seconds, interval.Milliseconds);
			var result = value.Value.Subtract(ts);
			return new SqlDayToSecond(result.Days, result.Hours, result.Minutes, result.Seconds, result.Milliseconds);
		}

		public byte[] ToByArray() {
			throw new NotImplementedException();
		}

		public bool Equals(SqlDayToSecond other) {
			if (IsNull && other.IsNull)
				return true;

			return value.Equals(other.value);
		}

		public SqlDayToSecond Negate() {
			if (IsNull || value == null)
				return this;

			var ts = value.Value.Negate();
			return new SqlDayToSecond(ts.Days, ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
		}

		public override bool Equals(object obj) {
			return Equals((SqlDayToSecond) obj);
		}

		public override int GetHashCode() {
			return value == null ? 0 : value.Value.GetHashCode();
		}

		public static SqlDayToSecond operator +(SqlDayToSecond a, SqlDayToSecond b) {
			return a.Add(b);
		}

		public static SqlDayToSecond operator -(SqlDayToSecond a, SqlDayToSecond b) {
			return a.Subtract(b);
		}

		public static bool operator ==(SqlDayToSecond a, SqlDayToSecond b) {
			return a.Equals(b);
		}

		public static bool operator !=(SqlDayToSecond a, SqlDayToSecond b) {
			return !(a == b);
		}

		public static SqlDayToSecond operator -(SqlDayToSecond value) {
			return value.Negate();
		}

		public override string ToString() {
			if (IsNull)
				return "NULL";

			return value.Value.ToString();
		}

		public static bool TryParse(string s, out SqlDayToSecond interval) {
			TimeSpan ts;
			if (!TimeSpan.TryParse(s, out ts)) {
				interval = Null;
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