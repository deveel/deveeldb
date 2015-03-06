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

namespace Deveel.Data.Sql {
	/// <summary>
	/// Defines the value of a <c>ROWID</c> object, that is a unique reference
	/// within a database system to a single row.
	/// </summary>
	public struct RowId : IEquatable<RowId> {
		/// <summary>
		/// Gets a <c>NULL</c> instance of <see cref="RowId"/>.
		/// </summary>
		public static readonly RowId Null = new RowId(true);

		/// <summary>
		/// Constructs the object with the references to the
		/// given table unique ID and the number of the row
		/// within the given table.
		/// </summary>
		/// <param name="tableId">The table unique identifier with the 
		/// database system.</param>
		/// <param name="rowNumber">The number of the row within the table.
		/// This value is always unique, also after the row is removed.</param>
		public RowId(int tableId, int rowNumber) 
			: this(false) {
			RowNumber = rowNumber;
			TableId = tableId;
		}

		private RowId(bool isNull)
			: this() {
			IsNull = isNull;
		}

		/// <summary>
		/// Gets the unique identifier of the table the row is contained.
		/// </summary>
		public int TableId { get; private set; }

		/// <summary>
		/// Gets the number of the column within the table referenced.
		/// </summary>
		public int RowNumber { get; private set; }

		/// <summary>
		/// Gets a boolean value indicating if the object equivales
		/// to a <c>NULL</c>.
		/// </summary>
		public bool IsNull { get; private set; }

		public bool Equals(RowId other) {
			if (IsNull && other.IsNull)
				return true;

			return TableId.Equals(other.TableId) &&
			       RowNumber.Equals(other.RowNumber);
		}

		public override bool Equals(object obj) {
			if (!(obj is RowId))
				return false;

			return Equals((RowId) obj);
		}

		public override int GetHashCode() {
			if (IsNull)
				return 0;

			return unchecked (TableId.GetHashCode() ^ RowNumber.GetHashCode());
		}

		public override string ToString() {
			return String.Format("{0}-{1}", TableId, RowNumber);
		}

		/// <summary>
		/// Attempts to parse the input string given into a valid
		/// instance of <see cref="RowId"/>.
		/// </summary>
		/// <param name="s">The input string to parse.</param>
		/// <param name="value">The out value from the parse.</param>
		/// <returns>
		/// Returns <c>true</c> if the string was succesfully parsed
		/// into a <see cref="RowId"/>, otherwise <c>false</c>.
		/// </returns>
		/// <seealso cref="ToString"/>
		public static bool TryParse(string s, out RowId value) {
			value = Null;

			if (String.IsNullOrEmpty(s))
				return false;

			var index = s.IndexOf("-", StringComparison.Ordinal);
			if (index == -1)
				return false;

			var s1 = s.Substring(0, index);
			var s2 = s.Substring(index + 1);

			int v1;
			int v2;
			if (!Int32.TryParse(s1, out v1))
				return false;
			if (!Int32.TryParse(s2, out v2))
				return false;

			value = new RowId(v1, v2);
			return true;
		}

		/// <summary>
		/// Parses the given input string into an instance of <see cref="RowId"/>.
		/// </summary>
		/// <param name="s">The input string to parse.</param>
		/// <returns>
		/// Returns a new instance of <see cref="RowId"/> as a result of the parse
		/// of the input string.
		/// </returns>
		/// <exception cref="FormatException">
		/// If the format of the input string is invalid.
		/// </exception>
		public static RowId Parse(string s) {
			RowId rowId;
			if (!TryParse(s, out rowId))
				throw new FormatException("Unable to parse the given string into a valid ROWID.");

			return rowId;
		}
	}
}
