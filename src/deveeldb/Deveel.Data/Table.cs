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
using System.Collections;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Threading;
using Deveel.Data.Types;
using Deveel.Diagnostics;

using SysMath = System.Math;

namespace Deveel.Data {
	/// <summary>
	/// This is a definition for a table in the database.
	/// </summary>
	/// <remarks>
	/// It stores the name of the table, and the fields (columns) in the 
	/// table.  A table represents either a 'core' <see cref="DataTable"/>
	/// that directly maps to the information stored in the database, or a 
	/// temporary table generated on the fly.
	/// <para>
	/// It is an abstract class, because it does not implement the methods to 
	/// add, remove or access row data in the table.
	/// </para>
	/// </remarks>
	public abstract partial class Table : ITableDataSource {

		/// <summary>
		/// Returns the <see cref="Database"/> object that this table is derived from.
		/// </summary>
		public abstract Database Database { get; }

		/// <summary>
		/// Returns the <see cref="TransactionSystem"/> object that this table is part of.
		/// </summary>
		public TransactionSystem System {
			get { return Database.System; }
		}

		/// <summary>
		/// Returns a <see cref="ILogger"/> object that we can use to log 
		/// debug messages to.
		/// </summary>
		protected internal virtual Logger Logger {
			get { return System.Logger; }
		}

		/// <summary>
		/// Returns the number of columns in the table.
		/// </summary>
		public abstract int ColumnCount { get; }

		/// <summary>
		/// Returns the number of rows stored in the table.
		/// </summary>
		public abstract int RowCount { get; }

		private VariableName ResolveColumnName(string columnName) {
			return VariableName.Resolve(TableInfo.TableName, columnName);
		}

		private VariableName[] ResolveColumnNames(string[] columnNames) {
			if (columnNames == null)
				return new VariableName[0];

			VariableName[] variableNames = new VariableName[columnNames.Length];
			for (int i = 0; i < columnNames.Length; i++) {
				variableNames[i] = ResolveColumnName(columnNames[i]);
			}

			return variableNames;
		}

		/// <summary>
		/// Returns a <see cref="TType"/> object that would represent 
		/// values at the given column index.
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		/// <exception cref="ApplicationException">
		/// If the column can't be found.
		/// </exception>
		public TType GetTTypeForColumn(int column) {
			return TableInfo[column].TType;
		}

		/// <summary>
		/// Returns a <see cref="TType"/> object that would represent 
		/// values in the given column.
		/// </summary>
		/// <param name="v"></param>
		/// <returns></returns>
		/// <exception cref="ApplicationException">
		/// If the column can't be found.
		/// </exception>
		public TType GetTTypeForColumn(VariableName v) {
			return GetTTypeForColumn(FindFieldName(v));
		}

		/// <summary>
		/// Given a fully qualified variable field name, this will 
		/// return the column index the field is at.
		/// </summary>
		/// <param name="v"></param>
		/// <returns></returns>
		public abstract int FindFieldName(VariableName v);


		/// <summary>
		/// Returns a fully qualified <see cref="VariableName"/> object 
		/// that represents the name of the column at the given index.
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public abstract VariableName GetResolvedVariable(int column);

		/// <summary>
		/// Returns a <see cref="SelectableScheme"/> for the given column 
		/// in the given <see cref="VirtualTable"/> row domain.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="originalColumn"></param>
		/// <param name="table"></param>
		/// <remarks>
		/// The <paramref name="column"/> variable may be modified as it traverses 
		/// through the tables, however the <paramref name="originalColumn"/>
		/// retains the link to the column in <paramref name="table"/>.
		/// </remarks>
		/// <returns></returns>
		internal abstract SelectableScheme GetSelectableSchemeFor(int column, int originalColumn, Table table);

		/// <summary>
		/// Given a set, this trickles down through the <see cref="Table"/> 
		/// hierarchy resolving the given <paramref name="rowSet"/>to a form 
		/// that the given ancestor understands.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="rowSet"></param>
		/// <param name="ancestor"></param>
		/// <remarks>
		/// Say you give the set { 0, 1, 2, 3, 4, 5, 6 }, this function may check
		/// down three levels and return a new 7 element set with the rows fully
		/// resolved to the given ancestors domain.
		/// </remarks>
		internal abstract void SetToRowTableDomain(int column, IList<int> rowSet, ITableDataSource ancestor);

		/// <summary>
		/// Return the list of <see cref="DataTable"/> and row sets that make up 
		/// the raw information in this table.
		/// </summary>
		/// <param name="info"></param>
		/// <returns></returns>
		internal abstract RawTableInformation ResolveToRawTable(RawTableInformation info);

		/// <summary>
		/// Returns an object that represents the information in the given 
		/// cell in the table.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="row"></param>
		/// <remarks>
		/// This will generally be an expensive algorithm, so calls to it should 
		/// be kept to a minimum.  Note that the offset between two rows is not 
		/// necessarily 1. Use <see cref="GetRowEnumerator"/> to get the contents 
		/// of a set.
		/// </remarks>
		/// <returns></returns>
		public abstract TObject GetCell(int column, int row);

		public TObject GetCell(VariableName columnName, int row) {
			return GetCell(FastFindFieldName(columnName), row);
		}

		public TObject GetCell(string columnName, int row) {
			return GetCell(ResolveColumnName(columnName), row);
		}

		/// <summary>
		/// Returns an <see cref="IEnumerator"/> of the rows in this table.
		/// </summary>
		/// <remarks>
		/// Each call to <see cref="IRowEnumerator.RowIndex"/> returns the 
		/// next valid row in the table. Note that the order that rows are retreived 
		/// depend on a number of factors. For a <see cref="DataTable"/> the rows are 
		/// accessed in the order they are in the data file.  For a <see cref="VirtualTable"/>, 
		/// the rows are accessed in the order of the last select operation.
		/// <para>
		/// If you want the rows to be returned by a specific column order then 
		/// use the <i>Sselec*</i> methods.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public abstract IRowEnumerator GetRowEnumerator();

		/// <summary>
		/// Returns a <see cref="TableInfo"/> object that defines the name 
		/// of the table and the layout of the columns of the table.
		/// </summary>
		/// <remarks>
		/// Note that for tables that are joined with other tables, the table name 
		/// and schema for this object become mangled.  For example, a table called 
		/// <c>PERSON</c> joined with a table called <c>MUSIC</c> becomes a table 
		/// called <c>PERSON#MUSIC</c> in a null schema.
		/// </remarks>
		public abstract DataTableInfo TableInfo { get; }


		/// <summary>
		/// Locks the root table(s) of this table so that it is impossible to
		/// overwrite the underlying rows that may appear in this table.
		/// </summary>
		/// <param name="lockKey">A given key that will also unlock the root table(s).</param>
		/// <remarks>
		/// This is used when cells in the table need to be accessed 'outside' 
		/// the Lock.  So we may have late access to cells in the table.
		/// <para>
		/// <b>Note</b>: This is nothing to do with the <see cref="LockingMechanism"/> object.
		/// </para>
		/// </remarks>
		public abstract void LockRoot(int lockKey);

		/// <summary>
		/// Unlocks the root tables so that the underlying rows may
		/// once again be used if they are not locked and have been removed.
		/// </summary>
		/// <param name="lockKey"></param>
		/// <remarks>
		/// This should be called some time after the rows have been locked.
		/// </remarks>
		public abstract void UnlockRoot(int lockKey);

		/// <summary>
		/// Returns true if the table has its row roots locked 
		/// (via the <see cref="LockRoot"/> method.
		/// </summary>
		public abstract bool HasRootsLocked { get; }

		// ---------- Implemented from ITableDataSource ----------

		/// <summary>
		/// Returns the <see cref="SelectableScheme"/> that indexes the 
		/// given column in this table.
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public SelectableScheme GetColumnScheme(int column) {
			return GetSelectableSchemeFor(column, column, this);
		}

		public SelectableScheme GetColumnScheme(VariableName columnName) {
			return GetColumnScheme(FastFindFieldName(columnName));
		}

		public SelectableScheme GetColumnScheme(string columnName) {
			return GetColumnScheme(ResolveColumnName(columnName));
		}

		// ---------- Convenience methods ----------

		/// <summary>
		/// Returns the <see cref="DataTableColumnInfo"/> object for the 
		/// given column index.
		/// </summary>
		/// <param name="columnOffset"></param>
		/// <returns></returns>
		public DataTableColumnInfo GetColumnInfo(int columnOffset) {
			return TableInfo[columnOffset];
		}


		/** ======================= Table Operations ========================= */

		/// <summary>
		/// Returns a table that is a merge of this table and the destination table.
		/// </summary>
		/// <param name="table"></param>
		/// <remarks>
		/// The rows that are in the destination table are included in this table.
		/// <para>
		/// The tables must have the same number of rows.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public Table ColumnMerge(Table table) {
			if (RowCount != table.RowCount) {
				throw new ApplicationException("Tables have different row counts.");
			}
			// Create the new VirtualTable with the joined tables.

			List<int> allRowSet = new List<int>();
			int rcount = RowCount;
			for (int i = 0; i < rcount; ++i) {
				allRowSet.Add(i);
			}

			Table[] tabs = new Table[] { this, table };
			IList<int>[] rowSets = new IList<int>[] { allRowSet, allRowSet };

			VirtualTable outTable = new VirtualTable(tabs);
			outTable.Set(tabs, rowSets);

			return outTable;
		}



		// ---------- The original table functions ----------

		/// <summary>
		/// Returns true if the given column number contains the value given.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="ob"></param>
		/// <returns></returns>
		public bool ColumnContainsValue(int column, TObject ob) {
			return ColumnMatchesValue(column, Operator.Get("="), ob);
		}

		public bool ColumnContainsValue(VariableName columnName, TObject value) {
			return ColumnContainsValue(FindFieldName(columnName), value);
		}

		public bool ColumnContainsValue(string columnName, TObject value) {
			return ColumnContainsValue(ResolveColumnName(columnName), value);
		}

		/// <summary>
		/// Returns true if the given column contains a value that the given
		/// operator returns true for with the given value.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="op"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		public bool ColumnMatchesValue(int column, Operator op, TObject value) {
			IList<int> rows = SelectRows(column, op, value);
			return (rows.Count > 0);
		}

		public bool ColumnMatchesValue(VariableName columnName, Operator op, TObject value) {
			return ColumnMatchesValue(FastFindFieldName(columnName), op, value);
		}

		public bool ColumnMatchesValue(string columnName, Operator op, TObject value) {
			return ColumnMatchesValue(ResolveColumnName(columnName), op, value);
		}

		/// <summary>
		/// Gets an object that can only access the cells that are in this
		/// table, and has no other access to the <see cref="Table"/> 
		/// functionalities.
		/// </summary>
		/// <remarks>
		/// The purpose of this object is to provide a clean way to access the state 
		/// of a table without being able to access any of the row sorting
		/// (SelectableScheme) methods that would return incorrect information in the
		/// situation where the table locks (via LockingMechanism) were removed.
		/// <para>
		/// <b>Note:</b> The methods in this class will only work if this table has 
		/// its rows locked via the <see cref="LockRoot"/> method.
		/// </para>
		/// </remarks>
		public TableAccessState GetTableAccessState() {
			return new TableAccessState(this);
		}


		/// <summary>
		/// Convenience, returns a TObject[] array given a single TObject, or
		/// null if the TObject is null (not if TObject represents a null value).
		/// </summary>
		/// <param name="cell"></param>
		/// <returns></returns>
		private static TObject[] SingleArrayCellMap(TObject cell) {
			return cell == null ? null : new TObject[] { cell };
		}

		/// <summary>
		/// Gets the first value of a column.
		/// </summary>
		/// <param name="column"></param>
		/// <returns>
		/// Returns the <see cref="TObject"/> value that represents the first item 
		/// in the set or <b>null</b> if there are no items in the column set.
		/// </returns>
		public TObject GetFirstCell(int column) {
			IList<int> rows = SelectFirst(column);
			return rows.Count > 0 ? GetCell(column, rows[0]) : null;
		}

		public TObject GetFirstCell(VariableName columnName) {
			return GetFirstCell(FastFindFieldName(columnName));
		}

		public TObject GetFirstCell(string columnName) {
			return GetFirstCell(ResolveColumnName(columnName));
		}

		/// <summary>
		/// Gets an array of the first values of the given columns.
		/// </summary>
		/// <param name="columns"></param>
		/// <returns>
		/// Returns the <see cref="TObject"/> values that represents the first items 
		/// in the set or <b>null</b> if there are no items in the column set.
		/// </returns>
		public TObject[] GetFirstCell(int[] columns) {
			if (columns.Length > 1)
				throw new ApplicationException("Multi-column GetLastCell not supported.");

			return SingleArrayCellMap(GetFirstCell(columns[0]));
		}

		public TObject[] GetFirstCell(params VariableName[] columnNames) {
			return GetFirstCell(FastFindFieldNames(columnNames));
		}

		/// <summary>
		/// Gets the last value of a column.
		/// </summary>
		/// <param name="column"></param>
		/// <returns>
		/// Returns the TObject value that represents the last item in the set or
		/// null if there are no items in the column set.
		/// </returns>
		public TObject GetLastCell(int column) {
			IList<int> rows = SelectLast(column);
			return rows.Count > 0 ? GetCell(column, rows[0]) : null;
		}

		public TObject GetLastCell(VariableName columnName) {
			return GetLastCell(FastFindFieldName(columnName));
		}

		public TObject GetLastCell(string columnName) {
			return GetLastCell(ResolveColumnName(columnName));
		}

		///<summary>
		/// Returns the TObject value that represents the last item in the set or
		/// null if there are no items in the column set.
		///</summary>
		///<param name="columns"></param>
		///<returns></returns>
		///<exception cref="ApplicationException"></exception>
		public TObject[] GetLastCell(int[] columns) {
			if (columns.Length > 1)
				throw new ApplicationException("Multi-column GetLastCellContent not supported.");

			return SingleArrayCellMap(GetLastCell(columns[0]));
		}

		/// <summary>
		/// If the given column contains all items of the same value, this method
		/// returns the value.
		/// </summary>
		/// <param name="column"></param>
		/// <returns>
		/// Returns the value of the column if all its the cells contains the
		/// same value, otherwise returns <b>null</b>.
		/// </returns>
		public TObject GetSingleCell(int column) {
			IList<int> rows = SelectFirst(column);
			int sz = rows.Count;
			return sz == RowCount && sz > 0 ? GetCell(column, rows[0]) : null;
		}

		///<summary>
		/// If the given column contains all items of the same value, this 
		/// method returns the value.
		///</summary>
		///<param name="columns"></param>
		///<returns></returns>
		/// <remarks>
		/// If it doesn't, or the column set is empty it returns null.
		/// </remarks>
		///<exception cref="ApplicationException"></exception>
		public TObject[] GetSingleCell(int[] columns) {
			if (columns.Length > 1)
				throw new ApplicationException("Multi-column GetSingleCellContent not supported.");

			return SingleArrayCellMap(GetSingleCell(columns[0]));
		}

		/// <summary>
		/// Converts the table to a <see cref="IDictionary{TKey,TValue}"/>.
		/// </summary>
		/// <returns>
		/// Returns the table as a <see cref="IDictionary"/>
		/// with the key/pair set.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If the table has more or less then two columns or if the first 
		/// column is not a string column.
		/// </exception>
		public Dictionary<string, object> ToDictionary() {
			if (ColumnCount != 2)
				throw new ApplicationException("Table must have two columns.");

			Dictionary<string, object> map = new Dictionary<string, object>();
			IRowEnumerator en = GetRowEnumerator();
			while (en.MoveNext()) {
				int rowIndex = en.RowIndex;
				TObject key = GetCell(0, rowIndex);
				TObject value = GetCell(1, rowIndex);
				map[key.Object.ToString()] = value.Object;
			}

			return map;
		}


		// Stores col name -> col index lookups
		private Dictionary<VariableName, int> colNameLookup;
		private readonly object colLookupLock = new object();

		/// <summary>
		/// Provides faster way to find a column index given a column name.
		/// </summary>
		/// <param name="col">Name of the column to get the index for.</param>
		/// <returns>
		/// Returns the index of the column for the given name, or -1
		/// if not found.
		/// </returns>
		public int FastFindFieldName(VariableName col) {
			lock (colLookupLock) {
				if (colNameLookup == null)
					colNameLookup = new Dictionary<VariableName, int>(30);

				int index;
				if (!colNameLookup.TryGetValue(col, out index)) {
					index = FindFieldName(col);
					colNameLookup[col] = index;
				}

				return index;
			}
		}

		private int[] FastFindFieldNames(params VariableName[] columnNames) {
			if (columnNames == null)
				return new int[0];

			int[] colIndex = new int[columnNames.Length];
			for (int i = 0; i < columnNames.Length; i++) {
				colIndex[i] = FastFindFieldName(columnNames[i]);
			}

			return colIndex;
		}

		/// <summary>
		/// Returns a TableVariableResolver object for this table.
		/// </summary>
		/// <returns></returns>
		internal TableVariableResolver GetVariableResolver() {
			return new TableVariableResolver(this);
		}


		// ---------- Inner classes ----------

		/// <summary>
		/// An implementation of <see cref="IVariableResolver"/> that we can use 
		/// to resolve column names in this table to cells for a specific row.
		/// </summary>
		internal class TableVariableResolver : IVariableResolver {
			public TableVariableResolver(Table table) {
				this.table = table;
			}

			private readonly Table table;
			private int rowIndex = -1;

			private int FindColumnName(VariableName variable) {
				int colIndex = table.FastFindFieldName(variable);
				if (colIndex == -1) {
					throw new ApplicationException("Can't find column: " + variable);
				}
				return colIndex;
			}

			// --- Implemented ---

			public int SetId {
				get { return rowIndex; }
				set { rowIndex = value; }
			}

			public TObject Resolve(VariableName variable) {
				return table.GetCell(FindColumnName(variable), rowIndex);
			}

			public TType ReturnTType(VariableName variable) {
				return table.GetTTypeForColumn(variable);
			}

		}

		/// <inheritdoc/>
		public override String ToString() {
			String name = "VT" + GetHashCode();
			if (this is DataTableBase) {
				name = ((DataTableBase)this).TableName.ToString();
			}
			return name;
		}

		/// <summary>
		/// Prints a graph of the table hierarchy to the stream.
		/// </summary>
		/// <param name="output"></param>
		/// <param name="indent"></param>
		public virtual void PrintGraph(TextWriter output, int indent) {
			for (int i = 0; i < indent; ++i) {
				output.Write(' ');
			}
			output.WriteLine("T[" + GetType() + "]");
		}
	}
}