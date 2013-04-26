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
using System.IO;

namespace Deveel.Data {
	/// <summary>
	/// A Table that represents the result of one or more other tables 
	/// joined together.
	/// </summary>
	public abstract class JoinedTable : Table {
		/// <summary>
		/// The list of tables that make up the join.
		/// </summary>
		private Table[] referenceList;

		/// <summary>
		/// The schemes to describe the entity relation in the given column.
		/// </summary>
		private SelectableScheme[] columnScheme;

		// These two arrays are lookup tables created in the constructor.  They allow
		// for quick resolution of where a given column should be 'routed' to in
		// the ancestors.

		/// <summary>
		/// Maps the column number in this table to the reference_list array to route to.
		/// </summary>
		private int[] columnTable;

		/// <summary>
		/// Gives a column filter to the given column to route correctly to the ancestor.
		/// </summary>
		private int[] columnFilter;

		/// <summary>
		/// The column that we are sorted against.
		/// </summary>
		/// <remarks>
		/// This is an optimization set by the <see cref="OptimisedPostSet"/> method.
		/// </remarks>
		private int sortedAgainstColumn = -1;

		/// <summary>
		/// The <see cref="TableInfo"/> object that describes the columns and name 
		/// of this table.
		/// </summary>
		private DataTableInfo vtTableInfo;

		/// <summary>
		/// Incremented when the roots are locked.
		/// </summary>
		/// <remarks>
		/// This should only ever be 1 or 0.
		/// </remarks>
		/// <seealso cref="LockRoot"/>
		/// <seealso cref="UnlockRoot"/>
		private byte rootsLocked;

		/// <summary>
		/// Constructs the <see cref="JoinedTable"/> with the list of tables in the parent.
		/// </summary>
		/// <param name="tables"></param>
		protected JoinedTable(Table[] tables) {
			CallInit(tables);
		}

		/// <summary>
		/// Constructs the <see cref="JoinedTable"/> with a single table.
		/// </summary>
		/// <param name="table"></param>
		internal JoinedTable(Table table)
			: this(new Table[] { table}) {
		}

		protected JoinedTable() {
		}

		private void CallInit(Table[] tables) {
			Init(tables);
		}

		/// <summary>
		/// Helper function for initializing the variables in the joined table.
		/// </summary>
		/// <param name="tables"></param>
		protected virtual void Init(Table[] tables) {
			int tableCount = tables.Length;
			referenceList = tables;

			int colCount = ColumnCount;
			columnScheme = new SelectableScheme[colCount];

			vtTableInfo = new DataTableInfo(new TableName(null, "#VIRTUAL TABLE#"));

			// Generate look up tables for column_table and column_filter information

			columnTable = new int[colCount];
			columnFilter = new int[colCount];
			int index = 0;
			for (int i = 0; i < referenceList.Length; ++i) {
				Table curTable = referenceList[i];
				DataTableInfo curTableInfo = curTable.TableInfo;
				int refColCount = curTable.ColumnCount;

				// For each column
				for (int n = 0; n < refColCount; ++n) {
					columnFilter[index] = n;
					columnTable[index] = i;
					++index;

					// Add this column to the data table info of this table.
					vtTableInfo.AddColumn(curTableInfo[n].Clone());
				}

			}

			vtTableInfo.IsReadOnly = true;
		}

		/// <summary>
		/// Returns a row reference list.
		/// </summary>
		/// <remarks>
		/// <b>Issue</b>: We should be able to optimise these types of things output.
		/// </remarks>
		/// <returns>
		/// Returns an <see cref="IList{T}"/> that represents a <i>reference</i> 
		/// to the rows in our virtual table.
		/// </returns>
		private IList<int> CalculateRowReferenceList() {
			int size = RowCount;
			List<int> allList = new List<int>(size);
			for (int i = 0; i < size; ++i) {
				allList.Add(i);
			}
			return allList;
		}

		/// <inheritdoc/>
		/// <remarks>
		/// We simply pick the first table to resolve the Database object.
		/// </remarks>
		public override Database Database {
			get { return referenceList[0].Database; }
		}

		/// <inheritdoc/>
		/// <remarks>
		/// This simply returns the column counts in the parent table(s).
		/// </remarks>
		public override int ColumnCount {
			get {
				int columnCountSum = 0;
				for (int i = 0; i < referenceList.Length; ++i) {
					columnCountSum += referenceList[i].ColumnCount;
				}
				return columnCountSum;
			}
		}

		/// <inheritdoc/>
		public override int FindFieldName(VariableName v) {
			int colIndex = 0;
			for (int i = 0; i < referenceList.Length; ++i) {
				int col = referenceList[i].FindFieldName(v);
				if (col != -1)
					return col + colIndex;

				colIndex += referenceList[i].ColumnCount;
			}
			return -1;
		}

		/// <inheritdoc/>
		public override VariableName GetResolvedVariable(int column) {
			Table parentTable = referenceList[columnTable[column]];
			return parentTable.GetResolvedVariable(columnFilter[column]);
		}

		/// <summary>
		/// Returns the list of <see cref="Table"/> objects that represent this 
		/// <see cref="VirtualTable"/>.
		/// </summary>
		protected Table[] ReferenceTables {
			get { return referenceList; }
		}

		/// <summary>
		/// This is an optimisation that should only be called <i>after</i> 
		/// a <i>set</i> method has been called.
		/// </summary>
		/// <param name="column"></param>
		/// <remarks>
		/// Because the <c>select</c> operation returns a set that is ordered by the 
		/// given column, we can very easily generate a <see cref="SelectableScheme"/> 
		/// object that can handle this column. So <paramref name="column"/> is the 
		/// column in which this virtual table is naturally ordered by.
		/// <para>
		/// The internals of this method may be totally commented output and the database 
		/// will still operate correctly. However this greatly speeds up situations when 
		/// you perform multiple consequtive operations on the same column.
		/// </para>
		/// </remarks>
		internal void OptimisedPostSet(int column) {
			sortedAgainstColumn = column;
		}

		/// <summary>
		/// Returns a <see cref="SelectableScheme"/> for the given column in the given 
		/// <see cref="VirtualTable"/> row domain.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="originalColumn"></param>
		/// <param name="table"></param>
		/// <remarks>
		/// This searches down through the tables ancestors until it comes across a table 
		/// with a <see cref="SelectableScheme"/> where the given column is fully resolved.
		/// In most cases, this will be the root <see cref="DataTable"/>.
		/// </remarks>
		/// <returns></returns>
		internal override SelectableScheme GetSelectableSchemeFor(int column, int originalColumn, Table table) {

			// First check if the given SelectableScheme is in the column_scheme array
			SelectableScheme scheme = columnScheme[column];
			if (scheme != null) {
				if (table == this)
					return scheme;

				return scheme.GetSubsetScheme(table, originalColumn);
			}

			// If it isn't then we need to calculate it
			SelectableScheme ss;

			// Optimization: The table may be naturally ordered by a column.  If it
			// is we don't try to generate an ordered set.
			if (sortedAgainstColumn != -1 &&
				sortedAgainstColumn == column) {
				InsertSearch isop = new InsertSearch(this, column, CalculateRowReferenceList());
				isop.RecordUid = false;
				ss = isop;
				columnScheme[column] = ss;
				if (table != this) {
					ss = ss.GetSubsetScheme(table, originalColumn);
				}

			} else {
				// Otherwise we must generate the ordered set from the information in
				// a parent index.
				Table parent_table = referenceList[columnTable[column]];
				ss = parent_table.GetSelectableSchemeFor(columnFilter[column], originalColumn, table);
				if (table == this) {
					columnScheme[column] = ss;
				}
			}

			return ss;
		}

		/// <inheritdoc/>
		internal override void SetToRowTableDomain(int column, IList<int> rowSet, ITableDataSource ancestor) {
			if (ancestor == this)
				return;

			int tableNum = columnTable[column];
			Table parentTable = referenceList[tableNum];

			// Resolve the rows into the parents indices.  (MANGLES row_set)
			ResolveAllRowsForTableAt(rowSet, tableNum);

			parentTable.SetToRowTableDomain(columnFilter[column], rowSet, ancestor);
		}

		/// <summary>
		/// Returns an object that contains fully resolved, one level only information 
		/// about the <see cref="DataTable"/> and the row indices of the data in this table.
		/// </summary>
		/// <param name="info"></param>
		/// <param name="rowSet"></param>
		/// <remarks>
		/// This information can be used to construct a new <see cref="VirtualTable"/>. We 
		/// need to supply an empty <see cref="RawTableInformation"/> object.
		/// </remarks>
		/// <returns></returns>
		private RawTableInformation ResolveToRawTable(RawTableInformation info, IList<int> rowSet) {
			if (this is IRootTable) {
				info.Add((IRootTable)this, CalculateRowReferenceList());
			} else {
				for (int i = 0; i < referenceList.Length; ++i) {

					List<int> newRowSet = new List<int>(rowSet);

					// Resolve the rows into the parents indices.
					ResolveAllRowsForTableAt(newRowSet, i);

					Table table = referenceList[i];
					if (table is IRootTable) {
						info.Add((IRootTable)table, newRowSet);
					} else {
						((JoinedTable)table).ResolveToRawTable(info, newRowSet);
					}
				}
			}

			return info;
		}

		/// <inheritdoc/>
		internal override RawTableInformation ResolveToRawTable(RawTableInformation info) {
			List<int> allList = new List<int>();
			int size = RowCount;
			for (int i = 0; i < size; ++i) {
				allList.Add(i);
			}
			return ResolveToRawTable(info, allList);
		}

		/// <summary>
		/// Returns the <see cref="TableInfo"/> object that describes the 
		/// columns in this table.
		/// </summary>
		/// <remarks>
		/// For a <see cref="VirtualTable"/>, this object contains the union of all 
		/// the columns in the children in the order set. The name of a virtual table i
		/// s the concat of all the parent table names. The schema is set to null.
		/// </remarks>
		public override DataTableInfo TableInfo {
			get { return vtTableInfo; }
		}

		/// <inheritdoc/>
		public override TObject GetCell(int column, int row) {
			int tableNum = columnTable[column];
			Table parentTable = referenceList[tableNum];
			row = ResolveRowForTableAt(row, tableNum);
			return parentTable.GetCell(columnFilter[column], row);
		}

		/// <inheritdoc/>
		public override IRowEnumerator GetRowEnumerator() {
			return new SimpleRowEnumerator(RowCount);
		}

		/// <inheritdoc/>
		public override void LockRoot(int lockKey) {
			// For each table, recurse.
			rootsLocked++;
			for (int i = 0; i < referenceList.Length; ++i) {
				referenceList[i].LockRoot(lockKey);
			}
		}

		/// <inheritdoc/>
		public override void UnlockRoot(int lockKey) {
			// For each table, recurse.
			rootsLocked--;
			for (int i = 0; i < referenceList.Length; ++i) {
				referenceList[i].UnlockRoot(lockKey);
			}
		}

		/// <inheritdoc/>
		public override bool HasRootsLocked {
			get { return rootsLocked != 0; }
		}

		/// <summary>
		/// The schemes to describe the entity relation in the given column.
		/// </summary>
		protected SelectableScheme[] ColumnScheme {
			get { return columnScheme; }
		}

		/// <summary>
		/// Maps the column number in this table to the reference_list array to route to.
		/// </summary>
		protected int[] ColumnTable {
			get { return columnTable; }
		}

		/// <summary>
		/// Gives a column filter to the given column to route correctly to the ancestor.
		/// </summary>
		protected int[] ColumnFilter {
			get { return columnFilter; }
		}

		/// <inheritdoc/>
		public override void PrintGraph(TextWriter output, int indent) {
			for (int i = 0; i < indent; ++i) {
				output.Write(' ');
			}
			output.WriteLine("JT[" + GetType());

			for (int i = 0; i < referenceList.Length; ++i) {
				referenceList[i].PrintGraph(output, indent + 2);
			}

			for (int i = 0; i < indent; ++i) {
				output.Write(' ');
			}
			output.WriteLine("]");
		}

		// ---------- Abstract methods ----------

		/// <summary>
		/// Given a row and a table index (to a parent reference table), this will
		/// return the row index in the given parent table for the given row.
		/// </summary>
		/// <param name="rowNumber"></param>
		/// <param name="tableNum"></param>
		/// <returns></returns>
		protected abstract int ResolveRowForTableAt(int rowNumber, int tableNum);

		/// <summary>
		/// Given an <see cref="IList{T}"/> that represents a list of pointers to rows 
		/// in this table, this resolves the rows to row indexes in the given parent table.
		/// </summary>
		/// <param name="rowSet"></param>
		/// <param name="tableNum"></param>
		/// <remarks>
		/// This method changes the <paramref name="rowSet"/> <see cref="IList{T}"/> object.
		/// </remarks>
		protected abstract void ResolveAllRowsForTableAt(IList<int> rowSet, int tableNum);
	}
}