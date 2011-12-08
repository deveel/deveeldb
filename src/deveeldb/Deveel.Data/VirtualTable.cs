// 
//  Copyright 2010  Deveel
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
using System.Collections.Generic;

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
		protected IList<int>[] row_list;

		/// <summary>
		/// The number of rows in the table.
		/// </summary>
		private int row_count;

		/// <inheritdoc/>
		protected override void Init(Table[] tables) {
			base.Init(tables);

			int table_count = tables.Length;
			row_list = new IList<int>[table_count];
			for (int i = 0; i < table_count; ++i) {
				row_list[i] = new List<int>();
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
		protected IList<int>[] ReferenceRows {
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
		internal void Set(Table table, IList<int> rows) {
			row_list[0] = new List<int>(rows);
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
		internal void Set(Table[] tables, IList<int>[] rows) {
			for (int i = 0; i < tables.Length; ++i) {
				row_list[i] = new List<int>(rows[i]);
			}
			if (rows.Length > 0) {
				row_count = rows[0].Count;
			}
		}

		///// <summary>
		///// Sets the rows in this table as above, but uses a <see cref="BlockIntegerList"/> 
		///// as an argument instead.
		///// </summary>
		///// <param name="table"></param>
		///// <param name="rows"></param>
		//internal void Set(Table table, BlockIntegerList rows) {
		//    row_list[0] = new List<int>(rows);
		//    row_count = rows.Count;
		//}

		///// <summary>
		///// Sets the rows in this table as above, but uses a <see cref="BlockIntegerList"/> 
		///// array as an argument instead.
		///// </summary>
		///// <param name="tables"></param>
		///// <param name="rows"></param>
		//internal void Set(Table[] tables, BlockIntegerList[] rows) {
		//    for (int i = 0; i < tables.Length; ++i) {
		//        row_list[i] = new IntegerVector(rows[i]);
		//    }
		//    if (rows.Length > 0) {
		//        row_count = rows[0].Count;
		//    }
		//}

		// ---------- Implemented from JoinedTable ----------

		protected override int ResolveRowForTableAt(int row_number, int table_num) {
			return row_list[table_num][row_number];
		}

		protected override void ResolveAllRowsForTableAt(IList<int> row_set, int table_num) {
			IList<int> cur_row_list = row_list[table_num];
			for (int n = row_set.Count - 1; n >= 0; --n) {
				int aa = row_set[n];
				int bb = cur_row_list[aa];
				row_set[n] = bb;
			}
		}
	}
}