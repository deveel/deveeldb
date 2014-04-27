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

using Deveel.Data.Index;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// Represents a default implementation of a <see cref="DataTable"/>.
	/// </summary>
	/// <remarks>
	/// It encapsulates information that is core to all <see cref="DataTable"/> 
	/// objects:
	/// <list type="bullet">
	/// <item>The table name</item>
	/// <item>The description of the table fields</item>
	/// <item>A set of <see cref="SelectableScheme"/> objects to describe row 
	/// relations</item>
	/// <item>A counter for the number of rows in the table</item>
	/// </list>
	/// <para>
	/// Extenders of this class can be tables directly mapping to internal 
	/// internal tables stored in the Database files (<see cref="DataTable"/>), 
	/// or tables that contains information generated on the fly by the DBMS 
	/// (<see cref="TemporaryTable"/>).
	/// </para>
	/// </remarks>
	public abstract class DefaultDataTable : DataTableBase {
		/// <summary>
		/// The Database object that this table is a child of.
		/// </summary>
		private readonly IDatabase database;

		/// <summary>
		/// The number of rows in the table.
		/// </summary>
		protected int row_count;

		/// <summary>
		/// A list of schemes for managing the data relations of each column.
		/// </summary>
		private SelectableScheme[] columnScheme;

		internal DefaultDataTable(IDatabase database) {
			this.database = database;
			row_count = 0;
		}

		/// <summary>
		/// Returns the Database object this table is part of.
		/// </summary>
		public override IDatabase Database {
			get { return database; }
		}

		/// <summary>
		/// Returns the <see cref="SelectableScheme"/> for the given column 
		/// index.
		/// </summary>
		/// <param name="column"></param>
		/// <remarks>
		/// This is different from <see cref="GetSelectableSchemeFor"/> because this 
		/// is designed to be overridden so derived classes can manage their own 
		/// <see cref="SelectableScheme"/> sources.
		/// </remarks>
		/// <returns></returns>
		protected virtual SelectableScheme GetRootColumnScheme(int column) {
			return columnScheme[column];
		}

		/// <summary>
		/// Clears the <see cref="SelectableScheme"/> information for the given 
		/// column index.
		/// </summary>
		/// <param name="column">Index of the clumn to clear the scheme.</param>
		protected void ClearColumnScheme(int column) {
			columnScheme[column] = null;
		}

		/// <summary>
		/// Blanks all the column schemes in the table to an initial state.
		/// </summary>
		/// <remarks>
		/// This will make all schemes of type <see cref="InsertSearch"/>.
		/// <para>
		/// <b>Note</b> The current default selectable scheme type is 
		/// <see cref="InsertSearch"/>. We may want to make this variable.
		/// </para>
		/// </remarks>
		protected void BlankSelectableSchemes() {
			BlankSelectableSchemes(0);
		}

		/// <summary>
		/// Blanks all the column schemes in this table to a specific 
		/// type of scheme.
		/// </summary>
		/// <param name="type">The type of the new scheme to set. If 0 
		/// then <see cref="InsertSearch"/> (fast but takes up memory - 
		/// requires each insert and delete from the table to be logged). 
		/// If 1 then <see cref="BlindSearch"/> (slower but uses no memory 
		/// and doesn't require insert and delete to be logged).</param>
		protected virtual void BlankSelectableSchemes(int type) {
			columnScheme = new SelectableScheme[ColumnCount];
			for (int i = 0; i < columnScheme.Length; ++i) {
				if (type == 0) {
					columnScheme[i] = new InsertSearch(this, i);
				} else if (type == 1) {
					columnScheme[i] = new BlindSearch(this, i);
				}
			}
		}

		/// <inheritdoc/>
		public override int ColumnCount {
			get { return TableInfo.ColumnCount; }
		}

		/// <inheritdoc/>
		public override int RowCount {
			get { return row_count; }
		}

		/// <inheritdoc/>
		public override VariableName GetResolvedVariable(int column) {
			String col_name = TableInfo[column].Name;
			return new VariableName(TableName, col_name);
		}

		/// <inheritdoc/>
		public override int FindFieldName(VariableName v) {
			// Check this is the correct table first...
			TableName tableName = v.TableName;
			DataTableInfo tableInfo = TableInfo;
			if (tableName != null && tableName.Equals(TableName)) {
				// Look for the column name
				string colName = v.Name;
				int size = ColumnCount;
				for (int i = 0; i < size; ++i) {
					DataColumnInfo col = tableInfo[i];
					if (col.Name.Equals(colName)) {
						return i;
					}
				}
			}
			return -1;
		}


		/// <inheritdoc/>
		internal override SelectableScheme GetSelectableSchemeFor(int column, int originalColumn, Table table) {
			SelectableScheme scheme = GetRootColumnScheme(column);

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
			List<int> row_set = new List<int>();
			IRowEnumerator e = GetRowEnumerator();
			while (e.MoveNext()) {
				row_set.Add(e.RowIndex);
			}
			info.Add(this, row_set);
			return info;
		}

		/* ===== Convenience methods for updating internal information =====
		   =============== regarding the SelectableSchemes ================= */

		/// <summary>
		/// Adds a single column of a row to the selectable scheme indexing.
		/// </summary>
		/// <param name="rowNumber"></param>
		/// <param name="columnNumber"></param>
		internal void AddCellToColumnSchemes(int rowNumber, int columnNumber) {
			bool indexableType = TableInfo[columnNumber].IsIndexableType;
			if (indexableType) {
				SelectableScheme ss = GetRootColumnScheme(columnNumber);
				ss.Insert(rowNumber);
			}
		}

		/// <summary>
		/// This is called when a row is in the table, and the SelectableScheme
		/// objects for each column need to be notified of the rows existance,
		/// therefore build up the relational model for the columns.
		/// </summary>
		/// <param name="rowNumber"></param>
		internal void AddRowToColumnSchemes(int rowNumber) {
			int colCount = ColumnCount;
			DataTableInfo tableInfo = TableInfo;
			for (int i = 0; i < colCount; ++i) {
				if (tableInfo[i].IsIndexableType) {
					SelectableScheme ss = GetRootColumnScheme(i);
					ss.Insert(rowNumber);
				}
			}
		}

		/// <summary>
		/// This is called when an index to a row needs to be removed from the
		/// SelectableScheme objects.
		/// </summary>
		/// <param name="rowNumber"></param>
		/// <remarks>
		/// This occurs when we have a modification log of row removals that haven't 
		/// actually happened to old backed up scheme.
		/// </remarks>
		internal void RemoveRowToColumnSchemes(int rowNumber) {
			int col_count = ColumnCount;
			DataTableInfo tableInfo = TableInfo;
			for (int i = 0; i < col_count; ++i) {
				if (tableInfo[i].IsIndexableType) {
					SelectableScheme ss = GetRootColumnScheme(i);
					ss.Remove(rowNumber);
				}
			}
		}

	}
}