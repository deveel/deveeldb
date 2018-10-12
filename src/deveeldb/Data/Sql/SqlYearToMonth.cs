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
	/// A month span representation of time.
	/// </summary>
	public struct SqlYearToMonth : ISqlValue, IComparable<SqlYearToMonth>, IEquatable<SqlYearToMonth>, ISqlFormattable {
		private readonly int months;

		public SqlYearToMonth(int months)
			: this() {
			this.months = months;
		}

		public SqlYearToMonth(int years, int months)
			: this((years * 12) + months) {
		}

		int IComparable.CompareTo(object obj) {
			if (!(obj is ISqlValue))
				throw new ArgumentException();

			return (this as IComparable<ISqlValue>).CompareTo((ISqlValue) obj);
		}

		int IComparable<ISqlValue>.CompareTo(ISqlValue other) {
			int i;
			if (other is SqlYearToMonth) {
				i = CompareTo((SqlYearToMonth) other);
			} else if (other is SqlNumber) {
				i = CompareTo((SqlNumber) other);
			} else {
				throw new NotSupportedException();
			}

			return i;
		}

		
		/// <summary>
		/// Gets the total number of months that represents the time span.
		/// </summary>
		public int TotalMonths {
			get {
				return months;
			}
		}

		/// <summary>
		/// Gets the total number of years that represents the time span.
		/// </summary>
		public double TotalYears {
			get {
				return ((double) months / 12);
			}
		}

		bool ISqlValue.IsComparableTo(ISqlValue other) {
			return other is SqlYearToMonth ||
			       other is SqlNumber;
		}

		public bool Equals(SqlYearToMonth other) {
			return months == other.months;
		}

		public override bool Equals(object obj) {
			if (!(obj is SqlYearToMonth))
				return false;

			return Equals((SqlYearToMonth)obj);
		}

		public override int GetHashCode() {
			return base.GetHashCode();
		}

		public SqlYearToMonth Add(SqlYearToMonth other) {
			return AddMonths(other.TotalMonths);
		}

		public SqlYearToMonth AddMonths(int value) {
			var result = months + value;
			return new SqlYearToMonth(result);
		}

		public SqlYearToMonth Subtract(SqlYearToMonth other) {
			return AddMonths(-other.TotalMonths);
		}

		/// <inheritdoc/>
		public int CompareTo(SqlYearToMonth other) {
			return months.CompareTo(other.months);
		}

		public int CompareTo(SqlNumber number) {
			var other = new SqlYearToMonth((int) number);
			return CompareTo(other);
		}

		public static SqlYearToMonth operator +(SqlYearToMonth a, SqlYearToMonth b) {
			return a.Add(b);
		}

		public static SqlYearToMonth operator -(SqlYearToMonth a, SqlYearToMonth b) {
			return a.Subtract(b);
		}

		public static bool operator ==(SqlYearToMonth a, SqlYearToMonth b) {
			return a.CompareTo(b) == 0;
		}

		public static bool operator !=(SqlYearToMonth a, SqlYearToMonth b) {
			return a.CompareTo(b) != 0;
		}

		public static bool operator >(SqlYearToMonth a, SqlYearToMonth b) {
			return a.CompareTo(b) > 0;
		}

		public static bool operator <(SqlYearToMonth a, SqlYearToMonth b) {
			return a.CompareTo(b) < 0;
		}

		public static bool operator >=(SqlYearToMonth a, SqlYearToMonth b) {
			return a.CompareTo(b) >= 0;
		}

		public static bool operator <=(SqlYearToMonth a, SqlYearToMonth b) {
			return a.CompareTo(b) <= 0;
		}


		public static explicit operator SqlYearToMonth(int value) {
			return new SqlYearToMonth(value);
		}


		public static explicit operator int(SqlYearToMonth value) {
			return value.months;
		}

		public static bool TryParse(string s, out SqlYearToMonth result) {
			Exception error;
			return TryParse(s, out result, out error);
		}

		private static bool TryParse(string s, out SqlYearToMonth result, out Exception error) {
			if (String.IsNullOrWhiteSpace(s)) {
				result = new SqlYearToMonth();
				error = new ArgumentNullException(nameof(s));
				return false;
			}

			int months;
			int years = 0;
			bool negative = false;
			if (s[0] == '-') {
				negative = true;
				s = s.Substring(1);

				if (String.IsNullOrWhiteSpace(s)) {
					error = new FormatException();
					result = new SqlYearToMonth();
					return false;
				}
			}

			var index = s.IndexOf('.');
			if (index != -1) {
				var ys = s.Substring(0, index);
				var ms = s.Substring(index + 1);

				if (!Int32.TryParse(ys, out years)) {
					error = new FormatException("Invalid digits for the years part in the interval string");
					result = new SqlYearToMonth();
					return false;
				}
				if (!Int32.TryParse(ms, out months)) {
					error = new FormatException("Invalid digits for the months in the interval string");
					result = new SqlYearToMonth();
					return false;
				}
			} else if (!Int32.TryParse(s, out months)) {
				error = new FormatException("Invalid digits for the months in the interval string");
				result = new SqlYearToMonth();
				return false;
			}

			var totalMonths = ((years * 12) + months);
			if (negative)
				totalMonths = -totalMonths;

			result = new SqlYearToMonth(totalMonths);
			error = null;
			return true;
		}

		public static SqlYearToMonth Parse(string s) {
			Exception error;
			SqlYearToMonth result;
			if (!TryParse(s, out result, out error))
				throw error;

			return result;
		}

		public override string ToString() {
			return this.ToSqlString();
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			var totalYears = TotalYears;
			var sign = System.Math.Sign(TotalMonths);
			var y = System.Math.Truncate(totalYears);
			var m = System.Math.Abs(TotalMonths - (y * 12));
			builder.AppendFormat("{0}{1}.{2}", (sign < 0) ? "-" : "", y, m);
		}
	}
}