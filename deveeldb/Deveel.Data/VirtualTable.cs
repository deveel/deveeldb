//  
//  VirtualTable.cs
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
	/// Representation of a table whose rows are actually physically 
	/// stored in another table.
	/// </summary>
	/// <remarks>
	/// In other words, this table just stores pointers to rows in other tables.
	/// <para>
	/// We use the VirtualTable to represent temporary tables created from select,
	/// join, etc operations.
	/// </para>
	/// <para>
	/// An important note about virtual tables:  performing a 'select' operation
	/// on a virtual table, unlike a <see cref="DataTable"/> that permanently stores 
	/// information about column cell relations resolves column relations 
	/// between the sub-set at select time. This involves asking the tables 
	/// parent(s) for a scheme to describe relations in a sub-set.
	/// </para>
	/// </remarks>
	public class VirtualTable : JoinedTable {

		/// <summary>
		/// Array of IntegerVectors that represent the rows taken from the given parents.
		/// </summary>
		protected IntegerVector[] row_list;

		/// <summary>
		/// The number of rows in the table.
		/// </summary>
		private int row_count;

		/// <inheritdoc/>
		protected override void Init(Table[] tables) {
			base.Init(tables);

			int table_count = tables.Length;
			row_list = new IntegerVector[table_count];
			for (int i = 0; i < table_count; ++i) {
				row_list[i] = new IntegerVector();
			}
		}

		/// <summary>
		/// Constructs the <see cref="VirtualTable"/> with a list of tables 
		/// that this virtual table is a sub-set or join of.
		/// </summary>
		/// <param name="tables"></param>
		internal VirtualTable(Table[] tables)
			: base(tables) {
		}

		internal VirtualTable(Table table)
			: base(table) {
		}

		protected VirtualTable()
			: base() {
		}

		/// <summary>
		/// Returns the list of <see cref="IntegerVector"/> that represents the rows 
		/// that this <see cref="VirtualTable"/> references.
		/// </summary>
		protected IntegerVector[] ReferenceRows {
			get { return row_list; }
		}

		/// <inheritdoc/>
		public override int RowCount {
			get { return row_count; }
		}


		/// <summary>
		/// Sets the rows in this table.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="rows"></param>
		/// <remarks>
		/// We should search for the <paramref name="table"/> in the 
		/// reference_list however we don't for efficiency.
		/// </remarks>
		internal void Set(Table table, IntegerVector rows) {
			row_list[0] = new IntegerVector(rows);
			row_count = rows.Count;
		}

		/// <summary>
		/// This is used in a join to set a list or joined rows and tables.
		/// </summary>
		/// <param name="tables"></param>
		/// <param name="rows"></param>
		/// <remarks>
		/// The <paramref name="tables"/> array should be an exact mirror of the 
		/// <i>reference_list</i>. The <paramref name="rows"/> array contains the 
		/// rows to add for each respective table. The given <see cref="IntegerVector"/> 
		/// objects should have identical lengths.
		/// </remarks>
		internal void Set(Table[] tables, IntegerVector[] rows) {
			for (int i = 0; i < tables.Length; ++i) {
				row_list[i] = new IntegerVector(rows[i]);
			}
			if (rows.Length > 0) {
				row_count = rows[0].Count;
			}
		}

		/// <summary>
		/// Sets the rows in this table as above, but uses a <see cref="BlockIntegerList"/> 
		/// as an argument instead.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="rows"></param>
		internal void Set(Table table, BlockIntegerList rows) {
			row_list[0] = new IntegerVector(rows);
			row_count = rows.Count;
		}

		/// <summary>
		/// Sets the rows in this table as above, but uses a <see cref="BlockIntegerList"/> 
		/// array as an argument instead.
		/// </summary>
		/// <param name="tables"></param>
		/// <param name="rows"></param>
		internal void Set(Table[] tables, BlockIntegerList[] rows) {
			for (int i = 0; i < tables.Length; ++i) {
				row_list[i] = new IntegerVector(rows[i]);
			}
			if (rows.Length > 0) {
				row_count = rows[0].Count;
			}
		}

		// ---------- Implemented from JoinedTable ----------

		protected override int ResolveRowForTableAt(int row_number, int table_num) {
			return row_list[table_num][row_number];
		}

		protected override void ResolveAllRowsForTableAt(IntegerVector row_set, int table_num) {
			IntegerVector cur_row_list = row_list[table_num];
			for (int n = row_set.Count - 1; n >= 0; --n) {
				int aa = row_set[n];
				int bb = cur_row_list[aa];
				row_set.SetIntAt(bb, n);
			}
		}
	}
}