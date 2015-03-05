// 
//  Copyright 2010-2014 Deveel
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

namespace Deveel.Data.DbSystem {
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
		private IList<int>[] rowList;

		/// <summary>
		/// The number of rows in the table.
		/// </summary>
		private int rowCount;

		/// <inheritdoc/>
		protected override void Init(Table[] tables) {
			base.Init(tables);

			int tableCount = tables.Length;
			rowList = new IList<int>[tableCount];
			for (int i = 0; i < tableCount; ++i) {
				rowList[i] = new List<int>();
			}
		}

		/// <summary>
		/// Constructs the <see cref="VirtualTable"/> with a list of tables 
		/// that this virtual table is a sub-set or join of.
		/// </summary>
		/// <param name="tables"></param>
		public VirtualTable(Table[] tables)
			: base(tables) {
		}

		public VirtualTable(Table table)
			: base(table) {
		}

		protected VirtualTable()
			: base() {
		}

		/// <summary>
		/// Returns the list of <see cref="IList{T}"/> that represents the rows 
		/// that this <see cref="VirtualTable"/> references.
		/// </summary>
		protected IList<int>[] ReferenceRows {
			get { return rowList; }
		}

		/// <inheritdoc/>
		public override int RowCount {
			get { return rowCount; }
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
			rowList[0] = new List<int>(rows);
			rowCount = rows.Count;
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
				rowList[i] = new List<int>(rows[i]);
			}
			if (rows.Length > 0) {
				rowCount = rows[0].Count;
			}
		}

		protected override int ResolveRowForTableAt(int rowNumber, int tableNum) {
			return rowList[tableNum][rowNumber];
		}

		protected override void ResolveAllRowsForTableAt(IList<int> rowSet, int tableNum) {
			IList<int> curRowList = rowList[tableNum];
			for (int n = rowSet.Count - 1; n >= 0; --n) {
				int aa = rowSet[n];
				int bb = curRowList[aa];
				rowSet[n] = bb;
			}
		}
	}
}