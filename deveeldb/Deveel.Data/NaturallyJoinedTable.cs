//  
//  NaturallyJoinedTable.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

using Deveel.Data.Collections;

namespace Deveel.Data {
	/// <summary>
	/// A table that is the cartesian product of two tables.
	/// </summary>
	/// <remarks>
	/// Provides better memory-use and efficiency than a materialized table 
	/// backed by a <see cref="VirtualTable"/>.
	/// </remarks>
	public sealed class NaturallyJoinedTable : JoinedTable {
		// The row counts of the left and right tables.
		private readonly int left_row_count, right_row_count;

		// The lookup row set for the left and right tables.  Basically, these point
		// to each row in either the left or right tables.
		private readonly IntegerVector left_set, right_set;
		private readonly bool left_is_simple_enum, right_is_simple_enum;

		///<summary>
		///</summary>
		///<param name="left"></param>
		///<param name="right"></param>
		public NaturallyJoinedTable(Table left, Table right) {
			base.Init(new Table[] { left, right });

			left_row_count = left.RowCount;
			right_row_count = right.RowCount;

			// Build lookup tables for the rows in the parent tables if necessary
			// (usually it's not necessary).

			// If the left or right tables are simple enumerations, we can optimize
			// our access procedure,
			left_is_simple_enum =
						(left.GetRowEnumerator() is SimpleRowEnumerator);
			right_is_simple_enum =
					   (right.GetRowEnumerator() is SimpleRowEnumerator);
			if (!left_is_simple_enum) {
				left_set = CreateLookupRowList(left);
			} else {
				left_set = null;
			}
			if (!right_is_simple_enum) {
				right_set = CreateLookupRowList(right);
			} else {
				right_set = null;
			}

		}

		/// <summary>
		/// Creates a lookup list for rows in the given table.
		/// </summary>
		/// <param name="t"></param>
		/// <returns></returns>
		private static IntegerVector CreateLookupRowList(ITableDataSource t) {
			IntegerVector ivec = new IntegerVector();
			IRowEnumerator en = t.GetRowEnumerator();
			while (en.MoveNext()) {
				int row_index = en.RowIndex;
				ivec.AddInt(row_index);
			}
			return ivec;
		}

		/// <summary>
		/// Given a row index between 0 and left table row count, this will return a
		/// row index into the left table's row domain.
		/// </summary>
		/// <param name="row_index"></param>
		/// <returns></returns>
		private int GetLeftRowIndex(int row_index) {
			if (left_is_simple_enum) {
				return row_index;
			}
			return left_set[row_index];
		}

		/// <summary>
		/// Given a row index between 0 and right table row count, this will return a
		/// row index into the right table's row domain.
		/// </summary>
		/// <param name="row_index"></param>
		/// <returns></returns>
		private int GetRightRowIndex(int row_index) {
			if (right_is_simple_enum) {
				return row_index;
			}
			return right_set[row_index];
		}



		// ---------- Implemented from JoinedTable ----------

		public override int RowCount {
			get {
				// Natural join row count is (left table row count * right table row count)
				return left_row_count*right_row_count;
			}
		}

		protected override int ResolveRowForTableAt(int row_number, int table_num) {
			if (table_num == 0) {
				return GetLeftRowIndex(row_number / right_row_count);
			} else {
				return GetRightRowIndex(row_number % right_row_count);
			}
		}

		protected override void ResolveAllRowsForTableAt(IntegerVector row_set, int table_num) {
			bool pick_right_table = (table_num == 1);
			for (int n = row_set.Count - 1; n >= 0; --n) {
				int aa = row_set[n];
				// Reverse map row index to parent domain
				int parent_row;
				if (pick_right_table) {
					parent_row = GetRightRowIndex(aa % right_row_count);
				} else {
					parent_row = GetLeftRowIndex(aa / right_row_count);
				}
				row_set.SetIntAt(parent_row, n);
			}
		}
	}
}