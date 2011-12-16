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

namespace Deveel.Data {
	/// <summary>
	/// A composite of two or more datasets used to implement <see cref="CompositeFunction.Union"/>, 
	/// <see cref="CompositeFunction.Intersect"/>, and <see cref="CompositeFunction.Except"/>.
	/// </summary>
	public class CompositeTable : Table, IRootTable {

		// ---------- Members ----------

		/// <summary>
		/// The 'master table' used to resolve information about this table such as
		/// fields and field types.
		/// </summary>
		private readonly Table masterTable;

		/// <summary>
		/// The tables being made a composite of.
		/// </summary>
		private readonly Table[] compositeTables;

		/// <summary>
		/// The list of indexes of rows to include in each table.
		/// </summary>
		private IList<int>[] tableIndexes;

		/// <summary>
		/// The schemes to describe the entity relation in the given column.
		/// </summary>
		private readonly SelectableScheme[] columnScheme;

		/// <summary>
		/// The number of root locks on this table.
		/// </summary>
		private int rootsLocked;

		/// <summary>
		/// Constructs the composite table given the <paramref name="masterTable"/> 
		/// (the column structure this composite table is based on), and 
		/// a list of tables to be the composite of this table.
		/// </summary>
		/// <param name="masterTable">The table defining the master structure 
		/// for the composition. this must be one of the tables listed in
		/// <paramref name="compositeList"/>.</param>
		/// <param name="compositeList">The list of tables to compose given 
		/// the structure of the master table.</param>
		/// <remarks>
		/// <b>Note:</b> This does not set up table indexes for a composite 
		/// function.
		/// </remarks>
		public CompositeTable(Table masterTable, Table[] compositeList) {
			this.masterTable = masterTable;
			compositeTables = compositeList;
			columnScheme = new SelectableScheme[masterTable.ColumnCount];
		}

		/// <summary>
		/// Consturcts the composite table assuming the first item in the 
		/// list is the master table.
		/// </summary>
		/// <param name="compositeList">The list of the tables to compose.</param>
		public CompositeTable(Table[] compositeList)
			: this(compositeList[0], compositeList) {
		}


		/// <summary>
		/// Removes duplicate rows from the table.
		/// </summary>
		/// <param name="preSorted">If <b>true</b>, each composite index 
		/// is already in sorted order.</param>
		private void RemoveDuplicates(bool preSorted) {
			throw new NotImplementedException();
		}

		/// <summary>
		/// Sets up the indexes in this composite table by performing for 
		/// composite function on the tables.
		/// </summary>
		/// <param name="function"></param>
		/// <param name="all">If <b>true</b>, duplicated rows are removed.</param>
		public void SetupIndexesForCompositeFunction(CompositeFunction function, bool all) {
			int size = compositeTables.Length;
			tableIndexes = new IList<int>[size];

			if (function == CompositeFunction.Union) {
				// Include all row sets in all tables
				for (int i = 0; i < size; ++i) {
					tableIndexes[i] = compositeTables[i].SelectAll();
				}

				if (!all)
					RemoveDuplicates(false);
			} else {
				throw new ApplicationException("Unrecognised composite function");
			}

		}

		// ---------- Implemented from Table ----------

		/// <inheritdoc/>
		public override Database Database {
			get { return masterTable.Database; }
		}

		/// <inheritdoc/>
		public override int ColumnCount {
			get { return masterTable.ColumnCount; }
		}

		/// <inheritdoc/>
		public override int RowCount {
			get {
				int rowCount = 0;
				for (int i = 0; i < tableIndexes.Length; ++i) {
					rowCount += tableIndexes[i].Count;
				}
				return rowCount;
			}
		}

		/// <inheritdoc/>
		public override int FindFieldName(VariableName v) {
			return masterTable.FindFieldName(v);
		}

		/// <inheritdoc/>
		public override DataTableInfo TableInfo {
			get { return masterTable.TableInfo; }
		}

		/// <inheritdoc/>
		public override VariableName GetResolvedVariable(int column) {
			return masterTable.GetResolvedVariable(column);
		}

		/// <inheritdoc/>
		internal override SelectableScheme GetSelectableSchemeFor(int column, int originalColumn, Table table) {
			SelectableScheme scheme = columnScheme[column];
			if (scheme == null) {
				scheme = new BlindSearch(this, column);
				columnScheme[column] = scheme;
			}

			// If we are getting a scheme for this table, simple return the information
			// from the column_trees Vector.
			if (table == this)
				return scheme;

			// Otherwise, get the scheme to calculate a subset of the given scheme.
			return scheme.GetSubsetScheme(table, originalColumn);
		}

		/// <inheritdoc/>
		internal override void SetToRowTableDomain(int column, IList<int> rowSet, ITableDataSource ancestor) {
			if (ancestor != this)
				throw new Exception("Method routed to incorrect table ancestor.");
		}

		/// <inheritdoc/>
		internal override RawTableInformation ResolveToRawTable(RawTableInformation info) {
			Console.Error.WriteLine("Efficiency Warning in DataTable.ResolveToRawTable.");
			List<int> row_set = new List<int>();
			IRowEnumerator e = GetRowEnumerator();
			while (e.MoveNext()) {
				row_set.Add(e.RowIndex);
			}
			info.Add(this, row_set);
			return info;
		}

		/// <inheritdoc/>
		public override TObject GetCellContents(int column, int row) {
			for (int i = 0; i < tableIndexes.Length; ++i) {
				IList<int> ivec = tableIndexes[i];
				int sz = ivec.Count;
				if (row < sz)
					return compositeTables[i].GetCellContents(column, ivec[row]);
				row -= sz;
			}
			throw new ApplicationException("Row '" + row + "' out of bounds.");
		}

		/// <inheritdoc/>
		public override IRowEnumerator GetRowEnumerator() {
			return new SimpleRowEnumerator(RowCount);
		}

		/// <inheritdoc/>
		public override void LockRoot(int lockKey) {
			// For each table, recurse.
			rootsLocked++;
			for (int i = 0; i < compositeTables.Length; ++i) {
				compositeTables[i].LockRoot(lockKey);
			}
		}

		/// <inheritdoc/>
		public override void UnlockRoot(int lock_key) {
			// For each table, recurse.
			rootsLocked--;
			for (int i = 0; i < compositeTables.Length; ++i) {
				compositeTables[i].UnlockRoot(lock_key);
			}
		}

		/// <inheritdoc/>
		public override bool HasRootsLocked {
			get { return rootsLocked != 0; }
		}

		// ---------- Implemented from IRootTable ----------

		/// <inheritdoc/>
		public bool TypeEquals(IRootTable table) {
			return (this == table);
			//    return true;
		}
	}
}