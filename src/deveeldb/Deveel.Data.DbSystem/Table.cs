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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Deveel.Data.Index;
using Deveel.Data.Threading;
using Deveel.Data.Types;
using Deveel.Diagnostics;

using SysMath = System.Math;

namespace Deveel.Data.DbSystem {
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
	public abstract class Table : ITableDataSource {

		/// <summary>
		/// Returns the <see cref="Database"/> object that this table is derived from.
		/// </summary>
		public abstract IDatabase Database { get; }

		/// <summary>
		/// Returns the <see cref="SystemContext"/> object that this table is part of.
		/// </summary>
		public ISystemContext Context {
			get { return Database.Context; }
		}

		/// <summary>
		/// Returns a <see cref="ILogger"/> object that we can use to log 
		/// debug messages to.
		/// </summary>
		protected internal virtual ILogger Logger {
			get { return Context.Logger; }
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
		/// Returns the <see cref="DataColumnInfo"/> object for the 
		/// given column index.
		/// </summary>
		/// <param name="columnOffset"></param>
		/// <returns></returns>
		public DataColumnInfo GetColumnInfo(int columnOffset) {
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

		#region Join

		/// <summary>
		/// Performs a natural join of this table with the given table.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="quick"></param>
		/// <remarks>
		///  This is the same as calling the <see cref="SimpleJoin"/> with no 
		/// conditional.
		/// </remarks>
		/// <returns></returns>
		public Table Join(Table table, bool quick) {
			Table outTable;

			if (quick) {
				// This implementation doesn't materialize the join
				outTable = new NaturallyJoinedTable(this, table);
			} else {

				Table[] tabs = new Table[2];
				tabs[0] = this;
				tabs[1] = table;
				IList<int>[] rowSets = new IList<int>[2];

				// Optimized trivial case, if either table has zero rows then result of
				// join will contain zero rows also.
				if (RowCount == 0 || table.RowCount == 0) {
					rowSets[0] = new List<int>(0);
					rowSets[1] = new List<int>(0);
				} else {
					// The natural join algorithm.
					List<int> thisRowSet = new List<int>();
					List<int> tableRowSet = new List<int>();

					// Get the set of all rows in the given table.
					List<int> tableSelectedSet = new List<int>();
					IRowEnumerator e = table.GetRowEnumerator();
					while (e.MoveNext()) {
						int rowIndex = e.RowIndex;
						tableSelectedSet.Add(rowIndex);
					}

					int tableSelectedSetSize = tableSelectedSet.Count;

					// Join with the set of rows in this table.
					e = GetRowEnumerator();
					while (e.MoveNext()) {
						int rowIndex = e.RowIndex;
						for (int i = 0; i < tableSelectedSetSize; ++i) {
							thisRowSet.Add(rowIndex);
						}
						tableRowSet.AddRange(tableSelectedSet);
					}

					// The row sets we are joining from each table.
					rowSets[0] = thisRowSet;
					rowSets[1] = tableRowSet;
				}

				// Create the new VirtualTable with the joined tables.
				VirtualTable virtTable = new VirtualTable(tabs);
				virtTable.Set(tabs, rowSets);

				outTable = virtTable;

			}

#if DEBUG
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, outTable + " = " + this + ".NaturalJoin(" + table + " )");
#endif

			return outTable;
		}

		/// <summary>
		/// Performs a natural join of this table with the given table.
		/// </summary>
		/// <param name="table"></param>
		/// <remarks>
		///  This is the same as calling the <see cref="SimpleJoin"/> with no 
		/// conditional.
		/// </remarks>
		/// <returns></returns>
		public Table Join(Table table) {
			return Join(table, true);
		}

		/// <summary>
		/// Finds all rows in this table that are <i>outside</i> the result
		/// in the given table.
		/// </summary>
		/// <param name="rightTable">The right table that must be a decendent of 
		/// this table.</param>
		/// <remarks>
		/// Performs a normal join, then determines unmatched joins.
		/// <para>
		/// It is possible to create an OuterTable with this result to make 
		/// the completed table.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public Table Outer(Table rightTable) {
			// Form the row list for right hand table,
			List<int> rowList = new List<int>(rightTable.RowCount);
			IRowEnumerator e = rightTable.GetRowEnumerator();
			while (e.MoveNext()) {
				rowList.Add(e.RowIndex);
			}

			int colIndex = rightTable.FindFieldName(GetResolvedVariable(0));
			rightTable.SetToRowTableDomain(colIndex, rowList, this);

			// This row set
			List<int> thisTableSet = new List<int>(RowCount);
			e = GetRowEnumerator();
			while (e.MoveNext()) {
				thisTableSet.Add(e.RowIndex);
			}

			// 'rowList' is now the rows in this table that are in 'rtable'.
			// Sort both 'thisTableSet' and 'rowList'
			thisTableSet.Sort();
			rowList.Sort();

			// Find all rows that are in 'this_table_set' and not in 'row_list'
			List<int> resultList = new List<int>(96);
			int size = thisTableSet.Count;
			int rowListIndex = 0;
			int rowListSize = rowList.Count;
			for (int i = 0; i < size; ++i) {
				int thisVal = thisTableSet[i];
				if (rowListIndex < rowListSize) {
					int inVal = rowList[rowListIndex];
					if (thisVal < inVal) {
						resultList.Add(thisVal);
					} else if (thisVal == inVal) {
						while (rowListIndex < rowListSize &&
							   rowList[rowListIndex] == inVal) {
							++rowListIndex;
						}
					} else {
						throw new ApplicationException("'this_val' > 'in_val'");
					}
				} else {
					resultList.Add(thisVal);
				}
			}

			// Return the new VirtualTable
			VirtualTable table = new VirtualTable(this);
			table.Set(this, resultList);

			return table;
		}

		/// <summary>
		/// Returns a new Table that is the union of the this table and 
		/// the given table.
		/// </summary>
		/// <param name="table"></param>
		/// <remarks>
		/// A union operation will remove any duplicate rows.
		/// </remarks>
		/// <returns></returns>
		public Table Union(Table table) {
			// Optimizations - handle trivial case of row count in one of the tables
			//   being 0.
			// NOTE: This optimization assumes this table and the unioned table are
			//   of the same type.
			if ((RowCount == 0 && table.RowCount == 0) ||
				 table.RowCount == 0) {

#if DEBUG
				if (Logger.IsInterestedIn(LogLevel.Information))
					Logger.Info(this, this + " = " + this + ".Union(" + table + " )");
#endif
				return this;
			}

			if (RowCount == 0) {
#if DEBUG
				if (Logger.IsInterestedIn(LogLevel.Information))
					Logger.Info(this, table + " = " + this + ".Union(" + table + " )");
#endif
				return table;
			}

			// First we merge this table with the input table.

			RawTableInformation raw1 = ResolveToRawTable(new RawTableInformation());
			RawTableInformation raw2 = table.ResolveToRawTable(new RawTableInformation());

			// This will throw an exception if the table types do not match up.

			raw1.Union(raw2);

			// Now 'raw1' contains a list of uniquely merged rows (ie. the union).
			// Now make it into a new table and return the information.

			Table[] tableList = raw1.GetTables();
			VirtualTable tableOut = new VirtualTable(tableList);
			tableOut.Set(tableList, raw1.GetRows());

#if DEBUG
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, tableOut + " = " + this + ".Union(" + table + " )");
#endif

			return tableOut;
		}

		/// <summary>
		/// A simple join operation.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="table"></param>
		/// <param name="columnName"></param>
		/// <param name="op"></param>
		/// <param name="expression"></param>
		/// <remarks>
		/// A simple join operation is one that has a single joining operator, 
		/// a <see cref="VariableName"/> on the lhs and a simple expression on the 
		/// rhs that includes only columns in the rhs table. For example, 
		/// <c>id = part_id</c> or <c>id == part_id * 2</c> or <c>id == part_id + vendor_id * 2</c>
		/// <para>
		/// It is important to understand how this algorithm works because all
		/// optimization of the expression must happen before the method starts.
		/// </para>
		/// <para>
		/// The simple join algorithm works as follows:  Every row of the right hand
		/// side table 'table' is iterated through.  The select opreation is applied
		/// to this table given the result evaluation.  Each row that matches is
		/// included in the result table.
		/// </para>
		/// <para>
		/// For optimal performance, the expression should be arranged so that the rhs
		/// table is the smallest of the two tables (because we must iterate through
		/// all rows of this table).  This table should be the largest.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public Table SimpleJoin(IQueryContext context, Table table, VariableName columnName, Operator op, Expression expression) {
			// Find the row with the name given in the condition.
			int lhsColumn = FindFieldName(columnName);

			if (lhsColumn == -1)
				throw new Exception("Unable to find the LHS column specified in the condition: " + columnName);

			// Create a variable resolver that can resolve columns in the destination
			// table.
			TableVariableResolver resolver = table.GetVariableResolver();

			// The join algorithm.  It steps through the RHS expression, selecting the
			// cells that match the relation from the LHS table (this table).

			List<int> thisRowSet = new List<int>();
			List<int> tableRowSet = new List<int>();

			IRowEnumerator e = table.GetRowEnumerator();

			while (e.MoveNext()) {
				int rowIndex = e.RowIndex;
				resolver.SetId = rowIndex;

				// Resolve expression into a constant.
				TObject value = expression.Evaluate(resolver, context);

				// Select all the rows in this table that match the joining condition.
				IList<int> selectedSet = SelectRows(lhsColumn, op, value);

				// Include in the set.
				int size = selectedSet.Count;
				for (int i = 0; i < size; ++i) {
					tableRowSet.Add(rowIndex);
				}
				thisRowSet.AddRange(selectedSet);

			}

			// Create the new VirtualTable with the joined tables.

			Table[] tabs = new Table[] { this, table };
			IList<int>[] rowSets = new IList<int>[] { thisRowSet, tableRowSet };

			VirtualTable outTable = new VirtualTable(tabs);
			outTable.Set(tabs, rowSets);

#if DEBUG
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, outTable + " = " + this + ".SimpleJoin(" + table + ", " + columnName + ", " + op + ", " + expression + " )");
#endif

			return outTable;
		}

		#endregion

		#region OrderBy

		/// <summary>
		/// Order the table by the given columns.
		/// </summary>
		/// <param name="columns">Column indices to order by the table.</param>
		/// <returns>
		/// Returns a table that is ordered by the given column numbers.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If the resultant table row count of the order differs from the 
		/// current table row count.
		/// </exception>
		public Table OrderByColumns(int[] columns) {
			// Sort by the column list.
			Table table = this;
			for (int i = columns.Length - 1; i >= 0; --i) {
				table = table.OrderByColumn(columns[i], true);
			}

			// A nice post condition to check on.
			if (RowCount != table.RowCount)
				throw new ApplicationException("Internal Error, row count != sorted row count");

			return table;
		}

		/// <summary>
		/// Gets an ordered list of rows.
		/// </summary>
		/// <param name="columns">Column indices to order by the rows.</param>
		/// <returns>
		/// Returns an <see cref="IList{T}"/> that represents the list of 
		/// rows in this table in sorted order by the given <paramref name="columns"/>.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If the resultant table row count of the order differs from the 
		/// current table row count.
		/// </exception>
		public IList<int> OrderedRowList(int[] columns) {
			Table work = OrderByColumns(columns);
			// 'work' is now sorted by the columns,
			// Get the rows in this tables domain,
			int rowCount = RowCount;
			List<int> rowList = new List<int>(rowCount);
			IRowEnumerator e = work.GetRowEnumerator();
			while (e.MoveNext()) {
				rowList.Add(e.RowIndex);
			}

			work.SetToRowTableDomain(0, rowList, this);
			return rowList;
		}


		/// <summary>
		/// Gets a table ordered by the column identified by <paramref name="columnIndex"/>.
		/// </summary>
		/// <param name="columnIndex">Index of the column to sort by.</param>
		/// <param name="ascending">Flag indicating the order direction (set <b>true</b> for
		/// ascending direction, <b>false</b> for descending).</param>
		/// <returns>
		/// Returns a Table which is identical to this table, except it is sorted by
		/// the column identified by <paramref name="columnIndex"/>.
		/// </returns>
		public VirtualTable OrderByColumn(int columnIndex, bool ascending) {
			// Check the field can be sorted
			DataColumnInfo colInfo = GetColumnInfo(columnIndex);

			List<int> rows = new List<int>(SelectAll(columnIndex));

			// Reverse the list if we are not ascending
			if (ascending == false)
				rows.Reverse();

			// We now has an int[] array of rows from this table to make into a
			// new table.

			VirtualTable table = new VirtualTable(this);
			table.Set(this, rows);

#if DEBUG
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, table + " = " + this + ".OrderByColumn(" + columnIndex + ", " + ascending + ")");
#endif

			return table;
		}

		/// <summary>
		/// Gets a table ordered by the column identified by <paramref name="column"/>.
		/// </summary>
		/// <param name="column">Name of the column to sort by.</param>
		/// <param name="ascending">Flag indicating the order direction (set <b>true</b> for
		/// ascending direction, <b>false</b> for descending).</param>
		/// <returns>
		/// Returns a Table which is identical to this table, except it is sorted by
		/// the column identified by <paramref name="column"/>.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If the given column name was not found.
		/// </exception>
		public VirtualTable OrderByColumn(VariableName column, bool ascending) {
			int colIndex = FindFieldName(column);
			if (colIndex == -1)
				throw new ApplicationException("Unknown column in 'OrderByColumn' ( " + column + " )");

			return OrderByColumn(colIndex, ascending);
		}

		public VirtualTable OrderByColumn(VariableName column) {
			return OrderByColumn(column, true);
		}

		#endregion

		#region Select

		/// <summary>
		/// Compares two instances with the given operator.
		/// </summary>
		/// <param name="ob1">First value to compare.</param>
		/// <param name="op">Operator for the comparation.</param>
		/// <param name="ob2">Second value to compare.</param>
		/// <returns>
		/// Returns a boolean value if the evaluation with the given
		/// operator of the two values is <see cref="Boolean"/>, 
		/// otherwise throw an exception.
		/// </returns>
		/// <exception cref="NullReferenceException">If the value returned by
		/// the evaluation is not a <see cref="Boolean"/>.</exception>
		private static bool CompareCells(TObject ob1, TObject ob2, Operator op) {
			TObject result = op.Evaluate(ob1, ob2, null, null, null);
			// NOTE: This will be a NullPointerException if the result is not a
			//   boolean type.
			//TODO: check...
			bool? bresult = result.ToNullableBoolean();
			if (!bresult.HasValue)
				throw new NullReferenceException();
			return bresult.Value;
		}

		/// <summary>
		/// Returns true if the given column contains all values that the given
		/// operator returns true for with the given value.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="op"></param>
		/// <param name="ob"></param>
		/// <returns></returns>
		internal bool AllColumnMatchesValue(int column, Operator op, TObject ob) {
			IList<int> rows = SelectRows(column, op, ob);
			return (rows.Count == RowCount);
		}

		/// <summary>
		/// Select all the rows of the table matching the given values for the
		/// given columns.
		/// </summary>
		/// <param name="cols"></param>
		/// <param name="op"></param>
		/// <param name="cells"></param>
		/// <remarks>
		/// Multi-select columns not yet supported.
		/// <para>
		/// <b>Note:</b> This can be used to exploit multi-column indexes 
		/// if they exist.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns a set that respresents the list of multi-column row numbers
		/// selected from the table given the condition.
		/// </returns>
		internal IList<int> SelectRows(int[] cols, Operator op, TObject[] cells) {
			// TODO: Look for an multi-column index to make this a lot faster,
			if (cols.Length > 1)
				throw new ApplicationException("Multi-column select not supported.");

			return SelectRows(cols[0], op, cells[0]);
		}

		/// <summary>
		/// Select all the rows of the table matching the given value for the
		/// given column.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="op"></param>
		/// <param name="cell"></param>
		/// <returns>
		/// Returns a set that respresents the list of row numbers
		/// selected from the table given the condition.
		/// </returns>
		internal IList<int> SelectRows(int column, Operator op, TObject cell) {
			// If the cell is of an incompatible type, return no results,
			TType colType = GetTTypeForColumn(column);
			if (!cell.TType.IsComparableType(colType)) {
				// Types not comparable, so return 0
				return new List<int>(0);
			}

			// Get the selectable scheme for this column
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);

			// If the operator is a standard operator, use the interned SelectableScheme
			// methods.
			if (op.IsEquivalent("="))
				return ss.SelectEqual(cell);
			if (op.IsEquivalent("<>"))
				return ss.SelectNotEqual(cell);
			if (op.IsEquivalent(">"))
				return ss.SelectGreater(cell);
			if (op.IsEquivalent("<"))
				return ss.SelectLess(cell);
			if (op.IsEquivalent(">="))
				return ss.SelectGreaterOrEqual(cell);
			if (op.IsEquivalent("<="))
				return ss.SelectLessOrEqual(cell);

			// If it's not a standard operator (such as IS, NOT IS, etc) we generate the
			// range set especially.
			SelectableRangeSet rangeSet = new SelectableRangeSet();
			rangeSet.Intersect(op, cell);
			return ss.SelectRange(rangeSet.ToArray());
		}

		/// <summary>
		/// Selects the rows in a table column between two minimum and maximum 
		/// bounds.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="minCell"></param>
		/// <param name="maxCell"></param>
		/// <remarks>
		/// <b>Note</b> The returns IntegerList <b>must</b> be sorted be the 
		/// <paramref name="column"/> cells.
		/// </remarks>
		/// <returns>
		/// Returns all the rows in the table with the value of <paramref name="column"/>
		/// column greater or equal then <paramref name="minCell"/> and smaller then
		/// <paramref name="maxCell"/>.
		/// </returns>
		public IList<int> SelectBetween(int column, TObject minCell, TObject maxCell) {
			// Check all the tables are comparable
			TType colType = GetTTypeForColumn(column);
			if (!minCell.TType.IsComparableType(colType) ||
				!maxCell.TType.IsComparableType(colType)) {
				// Types not comparable, so return 0
				return new List<int>(0);
			}

			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
			return ss.SelectBetween(minCell, maxCell);
		}

		/// <summary>
		/// This is the search method.</summary>
		/// <remarks>
		/// It requires a table to search, a column of the table, and a pattern.
		/// It returns the rows in the table that match the pattern if any. 
		/// Pattern searching only works successfully on columns that are of 
		/// type <see cref="DbType.String"/>. This works by first reducing the 
		/// search to all cells that contain the first section of text. ie. 
		/// <c>pattern = "Anto% ___ano"</c> will first reduce search to all 
		/// rows between <i>Anto</i> and <i>Anton</i>. This makes for better
		/// efficiency.
		/// </remarks>
		public IList<int> Search(int column, string pattern) {
			return Search(column, pattern, '\\');
		}

		/// <summary>
		/// This is the search method.
		/// </summary>
		/// <remarks>
		/// It requires a table to search, a column of the table, and a pattern.
		/// It returns the rows in the table that match the pattern if any. Pattern searching 
		/// only works successfully on columns that are of type DbType.String.
		/// This works by first reducing the search to all cells that contain the
		/// first section of text. ie. pattern = "Anto% ___ano" will first reduce
		/// search to all rows between "Anto" and "Anton".  This makes for better
		/// efficiency.
		/// </remarks>
		public IList<int> Search(int column, String pattern, char escapeChar) {
			// Get the type for the column
			TType colType = TableInfo[column].TType;

			// If the column type is not a string type then report an error.
			if (!(colType is TStringType))
				throw new ApplicationException("Unable to perform a pattern search on a non-String type column.");

			TStringType colStringType = (TStringType)colType;

			// ---------- Pre Search ----------

			// First perform a 'pre-search' on the head of the pattern.  Note that
			// there may be no head in which case the entire column is searched which
			// has more potential to be expensive than if there is a head.

			StringBuilder prePattern = new StringBuilder();
			int i = 0;
			bool finished = i >= pattern.Length;
			bool lastIsEscape = false;

			while (!finished) {
				char c = pattern[i];
				if (lastIsEscape) {
					lastIsEscape = true;
					prePattern.Append(c);
				} else if (c == escapeChar) {
					lastIsEscape = true;
				} else if (!PatternSearch.IsWildCard(c)) {
					prePattern.Append(c);

					++i;
					if (i >= pattern.Length) {
						finished = true;
					}

				} else {
					finished = true;
				}
			}

			// This is set with the remaining search.
			String postPattern;

			// This is our initial search row set.  In the second stage, rows are
			// eliminated from this vector.
			IList<int> searchCase;

			if (i >= pattern.Length) {
				// If the pattern has no 'wildcards' then just perform an EQUALS
				// operation on the column and return the results.

				TObject cell = new TObject(colType, pattern);
				return SelectRows(column, Operator.Get("="), cell);
			}

			if (prePattern.Length == 0 ||
			    colStringType.Locale != null) {

				// No pre-pattern easy search :-(.  This is either because there is no
				// pre pattern (it starts with a wild-card) or the locale of the string
				// is non-lexicographical.  In either case, we need to select all from
				// the column and brute force the search space.

				searchCase = SelectAll(column);
				postPattern = pattern;
			} else {

				// Criteria met: There is a pre_pattern, and the column locale is
				// lexicographical.

				// Great, we can do an upper and lower bound search on our pre-search
				// set.  eg. search between 'Geoff' and 'Geofg' or 'Geoff ' and
				// 'Geoff\33'

				String lowerBounds = prePattern.ToString();
				int nextChar = prePattern[i - 1] + 1;
				prePattern[i - 1] = (char)nextChar;
				String upperBounds = prePattern.ToString();

				postPattern = pattern.Substring(i);

				TObject cellLower = new TObject(colType, lowerBounds);
				TObject cellUpper = new TObject(colType, upperBounds);

				// Select rows between these two points.

				searchCase = SelectBetween(column, cellLower, cellUpper);
			}

			// ---------- Post search ----------

			int preIndex = i;

			// Now eliminate from our 'search_case' any cells that don't match our
			// search pattern.
			// Note that by this point 'post_pattern' will start with a wild card.
			// This follows the specification for the 'PatternMatch' method.
			// EFFICIENCY: This is a brute force iterative search.  Perhaps there is
			//   a faster way of handling this?

			var iList = new BlockIndex(searchCase);
			IIndexEnumerator enumerator = iList.GetEnumerator(0, iList.Count - 1);

			while (enumerator.MoveNext()) {

				// Get the expression (the contents of the cell at the given column, row)

				bool patternMatches = false;
				TObject cell = GetCell(column, enumerator.Current);
				// Null values doesn't match with anything
				if (!cell.IsNull) {
					String expression = cell.Object.ToString();
					// We must remove the head of the string, which has already been
					// found from the pre-search section.
					expression = expression.Substring(preIndex);
					patternMatches = PatternSearch.PatternMatch(postPattern, expression, escapeChar);
				}
				if (!patternMatches) {
					// If pattern does not match then remove this row from the search.
					enumerator.Remove();
				}

			}

			return iList.ToList();
		}

		/// <summary>
		/// Selects all the rows where the given column matches the regular
		/// expression.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="op"></param>
		/// <param name="ob"></param>
		/// <remarks>
		/// This uses the static class <see cref="PatternSearch"/> to 
		/// perform the operation.
		/// <para>
		/// This method must guarentee the result is ordered by the given 
		/// column index.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public IList<int> SelectFromRegex(int column, Operator op, TObject ob) {
			if (ob.IsNull)
				return new List<int>(0);

			string pattern = ob.Object.ToString();
			// If the first character is a '/' then we assume it's a Perl style regular
			// expression (eg. "/.*[0-9]+\/$/i")
			if (pattern.StartsWith("/")) {
				int end = pattern.LastIndexOf('/');
				String pat = pattern.Substring(1, end);
				String ops = pattern.Substring(end + 1);
				return Database.Context.RegexLibrary.RegexSearch(this, column, pat, ops);
			} else {
				// Otherwise it's a regular expression with no operators
				return Database.Context.RegexLibrary.RegexSearch(this, column, pattern, "");
			}
		}

		/// <summary>
		/// Selects all the rows where the given column matches the 
		/// given pattern.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="op">Operator for the selection (either <c>LIKE</c> 
		/// or <c>NOT LIKE</c>).</param>
		/// <param name="ob"></param>
		/// <remarks>
		/// This uses the static class <see cref="PatternSearch"/> to perform 
		/// these operations.
		/// </remarks>
		/// <returns></returns>
		public IList<int> SelectFromPattern(int column, Operator op, TObject ob) {
			if (ob.IsNull)
				return new List<int>();

			if (op.IsEquivalent("not like")) {
				// How this works:
				//   Find the set or rows that are like the pattern.
				//   Find the complete set of rows in the column.
				//   Sort the 'like' rows
				//   For each row that is in the original set and not in the like set,
				//     add to the result list.
				//   Result is the set of not like rows ordered by the column.
				List<int> likeSet = (List<int>)Search(column, ob.ToString());
				// Don't include NULL values
				TObject nullCell = new TObject(ob.TType, null);
				IList<int> originalSet = SelectRows(column, Operator.Get("is not"), nullCell);
				int listSize = SysMath.Max(4, (originalSet.Count - likeSet.Count) + 4);
				List<int> resultSet = new List<int>(listSize);
				likeSet.Sort();
				int size = originalSet.Count;
				for (int i = 0; i < size; ++i) {
					int val = originalSet[i];
					// If val not in like set, add to result
					if (likeSet.BinarySearch(val) == 0) {
						resultSet.Add(val);
					}
				}
				return resultSet;
			}

			// if (op.is("like")) {
			return Search(column, ob.ToString());
		}

		/// <summary>
		/// Returns a new table based on this table with no rows in it.
		/// </summary>
		/// <returns></returns>
		public Table EmptySelect() {
			if (RowCount == 0)
				return this;

			VirtualTable table = new VirtualTable(this);
			table.Set(this, new List<int>(0));
			return table;
		}

		/// <summary>
		/// Selects a single row at the given index from this table.
		/// </summary>
		/// <param name="rowIndex"></param>
		/// <returns></returns>
		public Table SingleRowSelect(int rowIndex) {
			VirtualTable table = new VirtualTable(this);
			List<int> ivec = new List<int>(1);
			ivec.Add(rowIndex);
			table.Set(this, ivec);
			return table;
		}

		public Table RangeSelect(string columnName, SelectableRange[] ranges) {
			return RangeSelect(ResolveColumnName(columnName), ranges);
		}

		/// <summary>
		/// A single column range select on this table.
		/// </summary>
		/// <param name="columnName">The column variable in this table (eg. Part.id)</param>
		/// <param name="ranges">The normalized (no overlapping) set of ranges to find.</param>
		/// <remarks>
		/// This can often be solved very quickly especially if there is an index 
		/// on the column.  The <see cref="SelectableRange"/> array represents a 
		/// set of ranges that are returned that meet the given criteria.
		/// </remarks>
		/// <returns></returns>
		public Table RangeSelect(VariableName columnName, SelectableRange[] ranges) {
			// If this table is empty then there is no range to select so
			// trivially return this object.
			if (RowCount == 0)
				return this;

			// Are we selecting a black or null range?
			if (ranges == null || ranges.Length == 0)
				// Yes, so return an empty table
				return EmptySelect();

			// Are we selecting the entire range?
			if (ranges.Length == 1 &&
				ranges[0].Equals(SelectableRange.FullRange))
				// Yes, so return this table.
				return this;

			// Must be a non-trivial range selection.

			// Find the column index of the column selected
			int column = FindFieldName(columnName);

			if (column == -1) {
				throw new Exception(
				   "Unable to find the column given to select the range of: " +
				   columnName.Name);
			}

			// Select the range
			IList<int> rows = SelectRange(column, ranges);

			// Make a new table with the range selected
			VirtualTable table = new VirtualTable(this);
			table.Set(this, rows);

			// We know the new set is ordered by the column.
			table.OptimisedPostSet(column);

#if DEBUG
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, table + " = " + this + ".RangeSelect(" + columnName + ", " + ranges + " )");
#endif

			return table;

		}

		/// <summary>
		/// A simple select on this table.
		/// </summary>
		/// <param name="context">The context of the query.</param>
		/// <param name="columnName">The left has side column reference.</param>
		/// <param name="op">The operator.</param>
		/// <param name="rhs">The expression to select against (the 
		/// expression <b>must</b> be a constant).</param>
		/// <remarks>
		/// We select against a column, with an <see cref="Operator"/> and a 
		/// rhs <see cref="Expression"/> that is constant (only needs to be 
		/// evaluated once).
		/// </remarks>
		/// <returns></returns>
		public Table SimpleSelect(IQueryContext context, VariableName columnName, Operator op, Expression rhs) {
			string debugSelectWith;

			// Find the row with the name given in the condition.
			int column = FindFieldName(columnName);

			if (column == -1) {
				throw new Exception(
				   "Unable to find the LHS column specified in the condition: " +
				   columnName.Name);
			}

			IList<int> rows;

			bool orderedBySelectColumn;

			// If we are doing a sub-query search
			if (op.IsSubQuery) {

				// We can only handle constant expressions in the RHS expression, and
				// we must assume that the RHS is a Expression[] array.
				object ob = rhs.Last;
				if (!(ob is TObject))
					throw new Exception("Sub-query not a TObject");

				TObject tob = (TObject)ob;
				if (tob.TType is TArrayType) {
					Expression[] list = (Expression[])tob.Object;

					// Construct a temporary table with a single column that we are
					// comparing to.
					DataColumnInfo col = GetColumnInfo(FindFieldName(columnName));
					DatabaseQueryContext dbContext = (DatabaseQueryContext)context;
					TemporaryTable ttable = new TemporaryTable(dbContext.Database, "single", new DataColumnInfo[] { col });

					foreach (Expression expression in list) {
						ttable.NewRow();
						ttable.SetRowObject(expression.Evaluate(null, null, context), 0);
					}

					ttable.SetupAllSelectableSchemes();

					// Perform the any/all sub-query on the constant table.

					return AnyAllNonCorrelated(new VariableName[] { columnName }, op, ttable);

				}

				throw new Exception("Error with format or RHS expression.");
			}

			// If we are doing a LIKE or REGEX pattern search
			if (op.IsEquivalent("like") ||
				op.IsEquivalent("not like") ||
				op.IsEquivalent("regex")) {

				// Evaluate the right hand side.  We know rhs is constant so don't
				// bother passing a IVariableResolver object.
				TObject value = rhs.Evaluate(null, context);

				if (op.IsEquivalent("regex")) {
					// Use the regular expression search to determine matching rows.
					rows = SelectFromRegex(column, op, value);
				} else {
					// Use the standard SQL pattern matching engine to determine
					// matching rows.
					rows = SelectFromPattern(column, op, value);
				}

				// These searches guarentee result is ordered by the column
				orderedBySelectColumn = true;

				// Describe the 'LIKE' select
#if DEBUG
				debugSelectWith = op + " " + value;
#endif

			}
				// Otherwise, we doing an index based comparison.
			else {

				// Is the column we are searching on indexable?
				DataColumnInfo colInfo = GetColumnInfo(column);
				if (!colInfo.IsIndexableType) {
					throw new StatementException("Can not search on field type " +
												 colInfo.TType.ToSqlString() +
												 " in '" + colInfo.Name + "'");
				}

				// Evaluate the right hand side.  We know rhs is constant so don't
				// bother passing a IVariableResolver object.
				TObject value = rhs.Evaluate(null, context);

				// Get the rows of the selected set that match the given condition.
				rows = SelectRows(column, op, value);
				orderedBySelectColumn = true;

				// Describe the select
#if DEBUG
				debugSelectWith = op + " " + value;
#endif

			}

			// We now has a set of rows from this table to make into a
			// new table.

			VirtualTable table = new VirtualTable(this);
			table.Set(this, rows);

			// OPTIMIZATION: Since we know that the 'select' return is ordered by the
			//   LHS column, we can easily generate a SelectableScheme for the given
			//   column.  This doesn't work for the non-constant set.

			if (orderedBySelectColumn) {
				table.OptimisedPostSet(column);
			}

#if DEBUG
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, table + " = " + this + ".SimpleSelect(" + columnName + " " + debugSelectWith + " )");
#endif

			return table;

		}

		/// <summary>
		/// Exhaustively searches through this table for rows that match 
		/// the expression given.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="expression"></param>
		/// <remarks>
		/// This is the slowest type of query and is not able to use any 
		/// type of indexing.
		/// <para>
		/// A <see cref="IQueryContext"/> object is used for resolving 
		/// sub-query plans.  If there are no sub-query plans in the 
		/// expression, this can safely be 'null'.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public Table ExhaustiveSelect(IQueryContext context, Expression expression) {
			Table result = this;

			// Exit early if there's nothing in the table to select from
			int rowCount = RowCount;
			if (rowCount > 0) {
				TableVariableResolver resolver = GetVariableResolver();
				IRowEnumerator e = GetRowEnumerator();

				List<int> selectedSet = new List<int>(rowCount);

				while (e.MoveNext()) {
					int rowIndex = e.RowIndex;
					resolver.SetId = rowIndex;

					// Resolve expression into a constant.
					TObject value = expression.Evaluate(resolver, context);

					// If resolved to true then include in the selected set.
					if (!value.IsNull && value.TType is TBooleanType &&
						value.Object.Equals(true)) {
						selectedSet.Add(rowIndex);
					}
				}

				// Make into a table to return.
				VirtualTable table = new VirtualTable(this);
				table.Set(this, selectedSet);

				result = table;
			}

#if DEBUG
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, result + " = " + this + ".ExhaustiveSelect(" + expression + " )");
#endif

			return result;
		}

		/// <summary>
		/// Evaluates a non-correlated ANY type operator given the LHS 
		/// expression, the RHS subquery and the ANY operator to use.
		/// </summary>
		/// <param name="context">The context of the query.</param>
		/// <param name="expression">The left has side expression. The <see cref="VariableName"/>
		/// objects in this expression must all reference columns in this table.</param>
		/// <param name="op">The operator to use.</param>
		/// <param name="rightTable">The subquery table should only contain 
		/// on column.</param>
		/// <remarks>
		/// ANY creates a new table that contains only the rows in this 
		/// table that the expression and operator evaluate to true for 
		/// any values in the given table.
		/// <para>
		/// The IN operator can be represented by using '= ANY'.
		/// </para>
		/// <para>
		/// Note that unlike the other join and select methods in this 
		/// object this will take a complex expression as the lhs provided 
		/// all the <see cref="VariableName"/> objects resolve to this table.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns the result of the ANY function on the table.
		/// </returns>
		public Table Any(IQueryContext context, Expression expression, Operator op, Table rightTable) {
			Table table = rightTable;

			// Check the table only has 1 column
			if (table.ColumnCount != 1)
				throw new ApplicationException("Input table <> 1 columns.");

			// Handle trivial case of no entries to select from
			if (RowCount == 0)
				return this;

			// If 'table' is empty then we return an empty set.  ANY { empty set } is
			// always false.
			if (table.RowCount == 0)
				return EmptySelect();

			// Is the lhs expression a constant?
			if (expression.IsConstant) {
				// We know lhs is a constant so no point passing arguments,
				TObject value = expression.Evaluate(null, context);
				// Select from the table.
				IList<int> list = table.SelectRows(0, op, value);
				if (list.Count > 0)
					// There's some entries so return the whole table,
					return this;

				// No entries matches so return an empty table.
				return EmptySelect();
			}

			Table sourceTable;
			int lhsColIndex;
			// Is the lhs expression a single variable?
			VariableName expVar = expression.AsVariableName();
			// NOTE: It'll be less common for this part to be called.
			if (expVar == null) {
				// This is a complex expression so make a FunctionTable as our new
				// source.
				FunctionTable funTable = new FunctionTable(this, new Expression[] { expression }, new String[] { "1" }, context);
				sourceTable = funTable;
				lhsColIndex = 0;
			} else {
				// The expression is an easy to resolve reference in this table.
				sourceTable = this;
				lhsColIndex = sourceTable.FindFieldName(expVar);
				if (lhsColIndex == -1) {
					throw new ApplicationException("Can't find column '" + expVar + "'.");
				}
			}

			// Check that the first column of 'table' is of a compatible type with
			// source table column (lhs_col_index).
			// ISSUE: Should we convert to the correct type via a FunctionTable?
			DataColumnInfo sourceCol = sourceTable.GetColumnInfo(lhsColIndex);
			DataColumnInfo destCol = table.GetColumnInfo(0);
			if (!sourceCol.TType.IsComparableType(destCol.TType)) {
				throw new ApplicationException("The type of the sub-query expression " +
								sourceCol.TType.ToSqlString() + " is incompatible " +
								"with the sub-query " + destCol.TType.ToSqlString() +
								".");
			}

			// We now have all the information to solve this query.
			// We work output as follows:
			//   For >, >= type ANY we find the lowest value in 'table' and
			//   select from 'source' all the rows that are >, >= than the
			//   lowest value.
			//   For <, <= type ANY we find the highest value in 'table' and
			//   select from 'source' all the rows that are <, <= than the
			//   highest value.
			//   For = type ANY we use same method from INHelper.
			//   For <> type ANY we iterate through 'source' only including those
			//   rows that a <> query on 'table' returns size() != 0.

			IList<int> selectRows;
			if (op.IsEquivalent(">") ||
				op.IsEquivalent(">=")) {
				// Select the first from the set (the lowest value),
				TObject lowestCell = table.GetFirstCell(0);
				// Select from the source table all rows that are > or >= to the
				// lowest cell,
				selectRows = sourceTable.SelectRows(lhsColIndex, op, lowestCell);
			} else if (op.IsEquivalent("<") ||
				op.IsEquivalent("<=")) {
				// Select the last from the set (the highest value),
				TObject highestCell = table.GetLastCell(0);
				// Select from the source table all rows that are < or <= to the
				// highest cell,
				selectRows = sourceTable.SelectRows(lhsColIndex, op, highestCell);
			} else if (op.IsEquivalent("=")) {
				// Equiv. to IN
				selectRows = sourceTable.In(table, lhsColIndex, 0);
			} else if (op.IsEquivalent("<>")) {
				// Select the value that is the same of the entire column
				TObject cell = table.GetSingleCell(0);
				if (cell != null) {
					// All values from 'source_table' that are <> than the given cell.
					selectRows = sourceTable.SelectRows(lhsColIndex, op, cell);
				} else {
					// No, this means there are different values in the given set so the
					// query evaluates to the entire table.
					return this;
				}
			} else {
				throw new ApplicationException("Don't understand operator '" + op + "' in ANY.");
			}

			// Make into a table to return.
			VirtualTable rtable = new VirtualTable(this);
			rtable.Set(this, selectRows);

#if DEBUG
			// Query logging information
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, rtable + " = " + this + ".Any(" + expression + ", " + op + ", " + rightTable + ")");
#endif

			return rtable;
		}

		/// <summary>
		/// Given a table and column (from this table), this returns all the rows
		/// from this table that are also in the first column of the given table.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="table"></param>
		/// <returns></returns>
		public IList<int> AllIn(int column, Table table) {
			return In(table, column, 0);
		}

		/// <summary>
		/// Given a table and column (from this table), this returns all the rows
		/// from this table that are not in the first column of the given table.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="table"></param>
		/// <returns></returns>
		public IList<int> AllNotIn(int column, Table table) {
			return NotIn(table, column, 0);
		}

		/// <summary>
		/// Evaluates a non-correlated ALL type operator given the LHS expression,
		/// the RHS subquery and the ALL operator to use.
		/// </summary>
		/// <param name="context">The context of the query.</param>
		/// <param name="expression">Expression containing <see cref="VariableName"/> 
		/// objects referencing columns in this table.</param>
		/// <param name="op">The operator to use.</param>
		/// <param name="table">The subquery table containing only one column.</param>
		/// <remarks>
		/// For example: <c>Table.col > ALL ( SELECT .... )</c>
		/// <para>
		/// ALL creates a new table that contains only the rows in this table that
		/// the expression and operator evaluate to true for all values in the
		/// given table.
		/// </para>
		/// <para>
		/// The <c>NOT IN</c> operator can be represented by using <c>&lt;&gt; ALL'</c>.
		/// </para>
		/// <para>
		/// Note that unlike the other join and select methods in this object this
		/// will take a complex expression as the lhs provided all the Variable
		/// objects resolve to this table.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns the result of the ALL function on the table.
		/// </returns>
		public Table All(IQueryContext context, Expression expression, Operator op, Table table) {
			// Check the table only has 1 column
			if (table.ColumnCount != 1)
				throw new ApplicationException("Input table <> 1 columns.");

			// Handle trivial case of no entries to select from
			if (RowCount == 0)
				return this;

			// If 'table' is empty then we return the complete set.  ALL { empty set }
			// is always true.
			if (table.RowCount == 0)
				return this;

			// Is the lhs expression a constant?
			if (expression.IsConstant) {
				// We know lhs is a constant so no point passing arguments,
				TObject value = expression.Evaluate(null, context);
				bool comparedToTrue;

				// The various operators
				if (op.IsEquivalent(">") || op.IsEquivalent(">=")) {
					// Find the maximum value in the table
					TObject cell = table.GetLastCell(0);
					comparedToTrue = CompareCells(value, cell, op);
				} else if (op.IsEquivalent("<") || op.IsEquivalent("<=")) {
					// Find the minimum value in the table
					TObject cell = table.GetFirstCell(0);
					comparedToTrue = CompareCells(value, cell, op);
				} else if (op.IsEquivalent("=")) {
					// Only true if rhs is a single value
					TObject cell = table.GetSingleCell(0);
					comparedToTrue = (cell != null && CompareCells(value, cell, op));
				} else if (op.IsEquivalent("<>")) {
					// true only if lhs_cell is not found in column.
					comparedToTrue = !table.ColumnContainsValue(0, value);
				} else {
					throw new ApplicationException("Don't understand operator '" + op + "' in ALL.");
				}

				// If matched return this table
				if (comparedToTrue)
					return this;

				// No entries matches so return an empty table.
				return EmptySelect();
			}

			Table sourceTable;
			int colIndex;
			// Is the lhs expression a single variable?
			VariableName expVar = expression.AsVariableName();
			// NOTE: It'll be less common for this part to be called.
			if (expVar == null) {
				// This is a complex expression so make a FunctionTable as our new
				// source.
				DatabaseQueryContext dbContext = (DatabaseQueryContext)context;
				FunctionTable funTable = new FunctionTable(
					  this, new Expression[] { expression }, new String[] { "1" }, dbContext);
				sourceTable = funTable;
				colIndex = 0;
			} else {
				// The expression is an easy to resolve reference in this table.
				sourceTable = this;
				colIndex = sourceTable.FindFieldName(expVar);
				if (colIndex == -1)
					throw new ApplicationException("Can't find column '" + expVar + "'.");
			}

			// Check that the first column of 'table' is of a compatible type with
			// source table column (lhs_col_index).
			// ISSUE: Should we convert to the correct type via a FunctionTable?
			DataColumnInfo sourceCol = sourceTable.GetColumnInfo(colIndex);
			DataColumnInfo destCol = table.GetColumnInfo(0);
			if (!sourceCol.TType.IsComparableType(destCol.TType))
				throw new ApplicationException("The type of the sub-query expression " +
											   sourceCol.TType.ToSqlString() + " is incompatible " +
											   "with the sub-query " + destCol.TType.ToSqlString() +
											   ".");

			// We now have all the information to solve this query.
			// We work output as follows:
			//   For >, >= type ALL we find the highest value in 'table' and
			//   select from 'source' all the rows that are >, >= than the
			//   highest value.
			//   For <, <= type ALL we find the lowest value in 'table' and
			//   select from 'source' all the rows that are <, <= than the
			//   lowest value.
			//   For = type ALL we see if 'table' contains a single value.  If it
			//   does we select all from 'source' that equals the value, otherwise an
			//   empty table.
			//   For <> type ALL we use the 'not in' algorithm.

			IList<int> selectList;
			if (op.IsEquivalent(">") || op.IsEquivalent(">=")) {
				// Select the last from the set (the highest value),
				TObject highestCell = table.GetLastCell(0);
				// Select from the source table all rows that are > or >= to the
				// highest cell,
				selectList = sourceTable.SelectRows(colIndex, op, highestCell);
			} else if (op.IsEquivalent("<") || op.IsEquivalent("<=")) {
				// Select the first from the set (the lowest value),
				TObject lowestCell = table.GetFirstCell(0);
				// Select from the source table all rows that are < or <= to the
				// lowest cell,
				selectList = sourceTable.SelectRows(colIndex, op, lowestCell);
			} else if (op.IsEquivalent("=")) {
				// Select the single value from the set (if there is one).
				TObject singleCell = table.GetSingleCell(0);
				if (singleCell != null) {
					// Select all from source_table all values that = this cell
					selectList = sourceTable.SelectRows(colIndex, op, singleCell);
				} else {
					// No single value so return empty set (no value in LHS will equal
					// a value in RHS).
					return EmptySelect();
				}
			} else if (op.IsEquivalent("<>")) {
				// Equiv. to NOT IN
				selectList = sourceTable.NotIn(table, colIndex, 0);
			} else {
				throw new ApplicationException("Don't understand operator '" + op + "' in ALL.");
			}

			// Make into a table to return.
			VirtualTable rtable = new VirtualTable(this);
			rtable.Set(this, selectList);

#if DEBUG
			// Query logging information
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, rtable + " = " + this + ".All(" + expression + ", " + op + ", " + table + ")");
#endif

			return rtable;
		}

		/// <summary>
		/// Returns an array that represents the sorted order of this table by
		/// the given column number.
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public IList<int> SelectAll(int column) {
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
			return ss.SelectAll();
		}

		/// <summary>
		/// Returns a list of rows that represents the enumerator order of 
		/// this table.
		/// </summary>
		/// <returns></returns>
		public IList<int> SelectAll() {
			List<int> list = new List<int>(RowCount);
			IRowEnumerator en = GetRowEnumerator();
			while (en.MoveNext()) {
				list.Add(en.RowIndex);
			}
			return list;
		}

		/// <summary>
		/// Returns a list that represents the sorted order of this table of all
		/// values in the given SelectableRange objects of the given column index.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="ranges"></param>
		/// <remarks>
		/// If there is an index on the column, the result can be found very quickly.
		/// The range array must be normalized (no overlapping ranges).
		/// </remarks>
		/// <returns></returns>
		public IList<int> SelectRange(int column, SelectableRange[] ranges) {
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
			return ss.SelectRange(ranges);
		}

		/// <summary>
		/// Returns a list that represents the last sorted element(s) of the given
		/// column index.
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public IList<int> SelectLast(int column) {
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
			return ss.SelectLast();
		}

		/// <summary>
		/// Returns a list that represents the first sorted element(s) of the given
		/// column index.
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public IList<int> SelectFirst(int column) {
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
			return ss.SelectFirst();
		}

		/// <summary>
		/// Returns a list that represents the rest of the sorted element(s) of the
		/// given column index (not the <i>first</i> set).
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public IList<int> SelectRest(int column) {
			SelectableScheme ss = GetSelectableSchemeFor(column, column, this);
			return ss.SelectNotFirst();
		}

		/// <summary>
		/// Returns a new table with any duplicate rows in 
		/// this table removed.
		/// </summary>
		/// <returns></returns>
		[Obsolete("Deprecated: not a proper SQL DISTINCT", false)]
		public VirtualTable Distinct() {
			RawTableInformation raw = ResolveToRawTable(new RawTableInformation());
			raw.removeDuplicates();

			Table[] tableList = raw.GetTables();
			VirtualTable tableOut = new VirtualTable(tableList);
			tableOut.Set(tableList, raw.GetRows());

#if DEBUG
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, tableOut + " = " + this + ".Distinct()");
#endif

			return tableOut;
		}

		/// <summary>
		/// Returns a new table that has only distinct rows in it.
		/// </summary>
		/// <param name="columns">Integer array containing the columns 
		/// to make distinct over.</param>
		/// <remarks>
		/// This is an expensive operation. We sort over all the columns, then 
		/// iterate through the result taking out any duplicate rows.
		/// <para>
		/// <b>Note</b>: This will change the order of this table in the result.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public Table Distinct(int[] columns) {
			List<int> resultList = new List<int>();
			IList<int> rowList = OrderedRowList(columns);

			int rowCount = rowList.Count;
			int previousRow = -1;
			for (int i = 0; i < rowCount; ++i) {
				int rowIndex = rowList[i];

				if (previousRow != -1) {

					bool equal = true;
					// Compare cell in column in this row with previous row.
					for (int n = 0; n < columns.Length && equal; ++n) {
						TObject c1 = GetCell(columns[n], rowIndex);
						TObject c2 = GetCell(columns[n], previousRow);
						equal = (c1.CompareTo(c2) == 0);
					}

					if (!equal) {
						resultList.Add(rowIndex);
					}
				} else {
					resultList.Add(rowIndex);
				}

				previousRow = rowIndex;
			}

			// Return the new table with distinct rows only.
			VirtualTable vt = new VirtualTable(this);
			vt.Set(this, resultList);

#if DEBUG
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, vt + " = " + this + ".distinct(" + columns + ")");
#endif

			return vt;
		}

		#endregion

		#region In

		/// <summary>
		/// This implements the <c>in</c> command.
		/// </summary>
		/// <param name="table1"></param>
		/// <param name="table2"></param>
		/// <param name="column1"></param>
		/// <param name="column2"></param>
		/// <returns>
		/// Returns the rows selected from <paramref name="table1"/>.
		/// </returns>
		public IList<int> In(Table table2, int column1, int column2) {
			// First pick the the smallest and largest table.  We only want to iterate
			// through the smallest table.
			// NOTE: This optimisation can't be performed for the 'not_in' command.

			Table smallTable;
			Table largeTable;
			int smallColumn;
			int largeColumn;

			if (RowCount < table2.RowCount) {
				smallTable = this;
				largeTable = table2;

				smallColumn = column1;
				largeColumn = column2;

			} else {
				smallTable = table2;
				largeTable = this;

				smallColumn = column2;
				largeColumn = column1;
			}

			// Iterate through the small table's column.  If we can find identical
			// cells in the large table's column, then we should include the row in our
			// final result.

			BlockIndex resultRows = new BlockIndex();
			IRowEnumerator e = smallTable.GetRowEnumerator();
			Operator op = Operator.Get("=");

			while (e.MoveNext()) {
				int smallRowIndex = e.RowIndex;
				TObject cell = smallTable.GetCell(smallColumn, smallRowIndex);

				IList<int> selectedSet = largeTable.SelectRows(largeColumn, op, cell);

				// We've found cells that are IN both columns,

				if (selectedSet.Count > 0) {
					// If the large table is what our result table will be based on, append
					// the rows selected to our result set.  Otherwise add the index of
					// our small table.  This only works because we are performing an
					// EQUALS operation.

					if (largeTable == this) {
						// Only allow unique rows into the table set.
						int sz = selectedSet.Count;
						bool rs = true;
						for (int i = 0; rs && i < sz; ++i) {
							rs = resultRows.UniqueInsertSort(selectedSet[i]);
						}
					} else {
						// Don't bother adding in sorted order because it's not important.
						resultRows.Add(smallRowIndex);
					}
				}
			}

			return new List<int>(resultRows);
		}

		/// <summary>
		/// A multi-column version of <c>IN</c>.
		/// </summary>
		/// <param name="table1"></param>
		/// <param name="table2"></param>
		/// <param name="t1Cols"></param>
		/// <param name="t2Cols"></param>
		/// <returns></returns>
		public IList<int> In(Table table2, int[] t1Cols, int[] t2Cols) {
			if (t1Cols.Length > 1)
				throw new NotSupportedException("Multi-column 'in' not supported yet.");

			return In(table2, t1Cols[0], t2Cols[0]);
		}

		/// <summary>
		/// This implements the <c>not in</c> command.
		/// </summary>
		/// <param name="table1"></param>
		/// <param name="table2"></param>
		/// <param name="col1"></param>
		/// <param name="col2"></param>
		/// <remarks>
		/// <b>Issue</b>: This will be less efficient than <see cref="In(Table,Table,int,int)">in</see> 
		/// if <paramref name="table1"/> has many rows and <paramref name="table2"/> has few rows.
		/// </remarks>
		/// <returns></returns>
		public IList<int> NotIn(Table table2, int col1, int col2) {
			// Handle trivial cases
			int t2_row_count = table2.RowCount;
			if (t2_row_count == 0)
				// No rows so include all rows.
				return SelectAll(col1);

			if (t2_row_count == 1) {
				// 1 row so select all from table1 that doesn't equal the value.
				IRowEnumerator en = table2.GetRowEnumerator();
				if (!en.MoveNext())
					throw new InvalidOperationException("Cannot iterate through table rows.");

				TObject cell = table2.GetCell(col2, en.RowIndex);
				return SelectRows(col1, Operator.Get("<>"), cell);
			}

			// Iterate through table1's column.  If we can find identical cell in the
			// tables's column, then we should not include the row in our final
			// result.
			List<int> resultRows = new List<int>();
			IRowEnumerator e = GetRowEnumerator();

			while (e.MoveNext()) {
				int rowIndex = e.RowIndex;
				TObject cell = GetCell(col1, rowIndex);

				IList<int> selectedSet = table2.SelectRows(col2, Operator.Equal, cell);

				// We've found a row in table1 that doesn't have an identical cell in
				// table2, so we should include it in the result.

				if (selectedSet.Count <= 0)
					resultRows.Add(rowIndex);
			}

			return resultRows;
		}

		/// <summary>
		/// A multi-column version of NOT IN.
		/// </summary>
		/// <param name="table1"></param>
		/// <param name="table2"></param>
		/// <param name="t1Cols"></param>
		/// <param name="t2Cols"></param>
		/// <returns></returns>
		public IList<int> NotIn(Table table2, int[] t1Cols, int[] t2Cols) {
			if (t1Cols.Length > 1)
				throw new NotSupportedException("Multi-column 'not in' not supported yet.");

			return NotIn(table2, t1Cols[0], t2Cols[0]);
		}

		#endregion

		/// <summary>
		/// The function for a non-correlated ANY or ALL sub-query operation between 
		/// a left and right branch.
		/// </summary>
		/// <param name="left_table"></param>
		/// <param name="left_vars"></param>
		/// <param name="op"></param>
		/// <param name="right_table"></param>
		/// <remarks>
		/// This function only works non-correlated sub-queries.
		/// <para>
		/// A non-correlated sub-query, or a correlated sub-query where the correlated
		/// variables are references to a parent plan branch, the plan only need be
		/// evaluated once and optimizations on the query present themselves.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		/// <example>
		/// An example of an SQL query that generates such a query is:
		/// <code>
		///    Table.col > ANY ( SELECT .... )
		/// </code>
		/// </example>
		public Table AnyAllNonCorrelated(VariableName[] left_vars, Operator op, Table right_table) {
			// Check the right table and the correct number of columns,
			if (right_table.ColumnCount != left_vars.Length) {
				throw new Exception("Input table <> " + left_vars.Length + " columns.");
			}

			// Handle trivial case of no entries to select from
			if (RowCount == 0) {
				return this;
			}

			// Resolve the vars in the left table and check the references are
			// compatible.
			int sz = left_vars.Length;
			int[] left_col_map = new int[sz];
			int[] right_col_map = new int[sz];
			for (int i = 0; i < sz; ++i) {
				left_col_map[i] = FindFieldName(left_vars[i]);
				right_col_map[i] = i;

				//      Console.Out.WriteLine("Finding: " + left_vars[i]);
				//      Console.Out.WriteLine("left_col_map: " + left_col_map[i]);
				//      Console.Out.WriteLine("right_col_map: " + right_col_map[i]);

				if (left_col_map[i] == -1) {
					throw new Exception("Invalid reference: " + left_vars[i]);
				}
				DataColumnInfo left_type = GetColumnInfo(left_col_map[i]);
				DataColumnInfo right_type = right_table.GetColumnInfo(i);
				if (!left_type.TType.IsComparableType(right_type.TType)) {
					throw new ApplicationException(
						"The type of the sub-query expression " + left_vars[i] + "(" +
						left_type.TType.ToSqlString() + ") is incompatible with " +
						"the sub-query type " + right_type.TType.ToSqlString() + ".");
				}
			}

			// We now have all the information to solve this query.

			IList<int> select_vec;

			if (op.IsSubQueryForm(OperatorSubType.All)) {
				// ----- ALL operation -----
				// We work out as follows:
				//   For >, >= type ALL we find the highest value in 'table' and
				//   select from 'source' all the rows that are >, >= than the
				//   highest value.
				//   For <, <= type ALL we find the lowest value in 'table' and
				//   select from 'source' all the rows that are <, <= than the
				//   lowest value.
				//   For = type ALL we see if 'table' contains a single value.  If it
				//   does we select all from 'source' that equals the value, otherwise an
				//   empty table.
				//   For <> type ALL we use the 'not in' algorithm.

				if (op.IsEquivalent(">") || op.IsEquivalent(">=")) {
					// Select the last from the set (the highest value),
					TObject[] highest_cells =
											right_table.GetLastCell(right_col_map);
					// Select from the source table all rows that are > or >= to the
					// highest cell,
					select_vec = SelectRows(left_col_map, op, highest_cells);
				} else if (op.IsEquivalent("<") || op.IsEquivalent("<=")) {
					// Select the first from the set (the lowest value),
					TObject[] lowest_cells =
										   right_table.GetFirstCell(right_col_map);
					// Select from the source table all rows that are < or <= to the
					// lowest cell,
					select_vec = SelectRows(left_col_map, op, lowest_cells);
				} else if (op.IsEquivalent("=")) {
					// Select the single value from the set (if there is one).
					TObject[] single_cell =
										 right_table.GetSingleCell(right_col_map);
					if (single_cell != null) {
						// Select all from source_table all values that = this cell
						select_vec = SelectRows(left_col_map, op, single_cell);
					} else {
						// No single value so return empty set (no value in LHS will equal
						// a value in RHS).
						return EmptySelect();
					}
				} else if (op.IsEquivalent("<>")) {
					// Equiv. to NOT IN
					select_vec = NotIn(right_table, left_col_map, right_col_map);
				} else {
					throw new Exception("Don't understand operator '" + op + "' in ALL.");
				}
			} else if (op.IsSubQueryForm(OperatorSubType.Any)) {

				// ----- ANY operation -----
				// We work out as follows:
				//   For >, >= type ANY we find the lowest value in 'table' and
				//   select from 'source' all the rows that are >, >= than the
				//   lowest value.
				//   For <, <= type ANY we find the highest value in 'table' and
				//   select from 'source' all the rows that are <, <= than the
				//   highest value.
				//   For = type ANY we use same method from INHelper.
				//   For <> type ANY we iterate through 'source' only including those
				//   rows that a <> query on 'table' returns size() != 0.

				if (op.IsEquivalent(">") || op.IsEquivalent(">=")) {
					// Select the first from the set (the lowest value),
					TObject[] lowest_cells =
										   right_table.GetFirstCell(right_col_map);
					// Select from the source table all rows that are > or >= to the
					// lowest cell,
					select_vec = SelectRows(left_col_map, op, lowest_cells);
				} else if (op.IsEquivalent("<") || op.IsEquivalent("<=")) {
					// Select the last from the set (the highest value),
					TObject[] highest_cells =
											right_table.GetLastCell(right_col_map);
					// Select from the source table all rows that are < or <= to the
					// highest cell,
					select_vec = SelectRows(left_col_map, op, highest_cells);
				} else if (op.IsEquivalent("=")) {
					// Equiv. to IN
					select_vec = In(right_table, left_col_map, right_col_map);
				} else if (op.IsEquivalent("<>")) {
					// Select the value that is the same of the entire column
					TObject[] cells = right_table.GetSingleCell(right_col_map);
					if (cells != null) {
						// All values from 'source_table' that are <> than the given cell.
						select_vec = SelectRows(left_col_map, op, cells);
					} else {
						// No, this means there are different values in the given set so the
						// query evaluates to the entire table.
						return this;
					}
				} else {
					throw new Exception("Don't understand operator '" + op + "' in ANY.");
				}
			} else {
				throw new Exception("Unrecognised sub-query operator.");
			}

			// Make into a table to return.
			VirtualTable rtable = new VirtualTable(this);
			rtable.Set(this, select_vec);

			return rtable;
		}
	}
}