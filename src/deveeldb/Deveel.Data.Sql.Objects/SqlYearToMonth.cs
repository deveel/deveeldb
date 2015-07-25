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

namespace Deveel.Data.Sql.Objects {
	/// <summary>
	/// A month span representation of time.
	/// </summary>
	public struct SqlYearToMonth : ISqlObject, IComparable<SqlYearToMonth> {
		private int? months;

		public static readonly SqlYearToMonth Null = new SqlYearToMonth(true);

		public SqlYearToMonth(int months) 
			: this() {
			this.months = months;
		}

		public SqlYearToMonth(int years, int months)
			: this((years*12) + months) {
		}

		private SqlYearToMonth(bool isNull)
			: this() {
			if (isNull)
				months = null;
		}

		int IComparable.CompareTo(object obj) {
			return CompareTo((ISqlObject) obj);
		}

		public int CompareTo(ISqlObject other) {
			if (other is SqlYearToMonth)
				return CompareTo((SqlYearToMonth) other);

			throw new NotSupportedException();
		}

		/// <inheritdoc/>
		public bool IsNull {
			get { return months == null; }
		}

		/// <summary>
		/// Gets the total number of months that represents the time span.
		/// </summary>
		public int TotalMonths {
			get {
				if (months == null)
					throw new NullReferenceException();

				return months.Value;
			}
		}

		/// <summary>
		/// Gets the total number of years that represents the time span.
		/// </summary>
		public double TotalYears {
			get {
				if (months == null)
					throw new NullReferenceException();

				return (months.Value/12);
			}
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return other is SqlYearToMonth ||
			       other is SqlNumber ||
			       other is SqlDayToSecond;
		}

		/// <inheritdoc/>
		public int CompareTo(SqlYearToMonth other) {
			if (other.IsNull && IsNull)
				return 0;
			if (IsNull && !other.IsNull)
				return 1;
			if (!IsNull && other.IsNull)
				return -1;

			return months.Value.CompareTo(other.months.Value);
		}
	}
}