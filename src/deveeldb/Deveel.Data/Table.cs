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

using Deveel.Data.Collections;
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
		private Dictionary<VariableName, int> columnNameLookup;
		private readonly object columnLookupLock = new object();

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
		/// Returns a <see cref="IDebugLogger"/> object that we can use to log 
		/// debug messages to.
		/// </summary>
		protected internal virtual IDebugLogger Debug {
			get { return System.Debug; }
		}

		/// <summary>
		/// Returns the number of columns in the table.
		/// </summary>
		public abstract int ColumnCount { get; }

		/// <summary>
		/// Returns the number of rows stored in the table.
		/// </summary>
		public abstract int RowCount { get; }

		/// <summary>
		/// Returns a <see cref="DataTableInfo"/> object that defines the name 
		/// of the table and the layout of the columns of the table.
		/// </summary>
		/// <remarks>
		/// Note that for tables that are joined with other tables, the table name 
		/// and schema for this object become mangled.  For example, a table called 
		/// <c>PERSON</c> joined with a table called <c>MUSIC</c> becomes a table 
		/// called <c>PERSON#MUSIC</c> in a null schema.
		/// </remarks>
		public abstract DataTableInfo DataTableInfo { get; }

		/// <summary>
		/// Returns true if the table has its row roots locked 
		/// (via the <see cref="LockRoot"/> method.
		/// </summary>
		public abstract bool HasRootsLocked { get; }

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
			return DataTableInfo[column].TType;
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
		/// hierarchy resolving the given row set to a form that the given 
		/// ancestor understands.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="rowSet"></param>
		/// <param name="ancestor"></param>
		/// <remarks>
		/// Say you give the set { 0, 1, 2, 3, 4, 5, 6 }, this function may check
		/// down three levels and return a new 7 element set with the rows fully
		/// resolved to the given ancestors domain.
		/// </remarks>
		internal abstract void SetToRowTableDomain(int column, IntegerVector rowSet, ITableDataSource ancestor);

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
		public abstract TObject GetCellContents(int column, int row);

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


		/// <summary>
		/// Returns the <see cref="DataTableColumnInfo"/> object for the 
		/// given column index.
		/// </summary>
		/// <param name="col_index"></param>
		/// <returns></returns>
		public DataTableColumnInfo GetColumn(int col_index) {
			return DataTableInfo[col_index];
		}

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
			if (RowCount != table.RowCount)
				throw new ApplicationException("Tables have different row counts.");

			// Create the new VirtualTable with the joined tables.

			IntegerVector allRowSet = new IntegerVector();
			int rcount = RowCount;
			for (int i = 0; i < rcount; ++i) {
				allRowSet.AddInt(i);
			}

			Table[] tabs = new Table[] {this, table};
			IntegerVector[] row_sets = new IntegerVector[] {allRowSet, allRowSet};

			VirtualTable outTable = new VirtualTable(tabs);
			outTable.Set(tabs, row_sets);

			return outTable;
		}


		// ---------- Queries using Expression class ----------

		/// <summary>
		/// Returns true if the given column number contains the value given.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="ob"></param>
		/// <returns></returns>
		public bool ColumnContainsValue(int column, TObject ob) {
			return ColumnMatchesValue(column, Operator.Get("="), ob);
		}

		/// <summary>
		/// Returns true if the given column contains a value that the given
		/// operator returns true for with the given value.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="op"></param>
		/// <param name="ob"></param>
		/// <returns></returns>
		public bool ColumnMatchesValue(int column, Operator op, TObject ob) {
			IntegerVector ivec = SelectRows(column, op, ob);
			return (ivec.Count > 0);
		}

		/// <summary>
		/// Returns true if the given column contains all values that the given
		/// operator returns true for with the given value.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="op"></param>
		/// <param name="ob"></param>
		/// <returns></returns>
		public bool AllColumnMatchesValue(int column, Operator op, TObject ob) {
			IntegerVector ivec = SelectRows(column, op, ob);
			return (ivec.Count == RowCount);
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
		/// Given a table and column (from this table), this returns all the rows
		/// from this table that are also in the first column of the given table.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="table"></param>
		/// <returns></returns>
		internal virtual IntegerVector AllRowsIn(int column, Table table) {
			return INHelper.In(this, table, column, 0);
		}

		/// <summary>
		/// Given a table and column (from this table), this returns all the rows
		/// from this table that are not in the first column of the given table.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="table"></param>
		/// <returns></returns>
		internal virtual IntegerVector AllRowsNotIn(int column, Table table) {
			return INHelper.NotIn(this, table, column, 0);
		}

		/// <summary>
		/// Convenience, returns a TObject[] array given a single TObject, or
		/// null if the TObject is null (not if TObject represents a null value).
		/// </summary>
		/// <param name="cell"></param>
		/// <returns></returns>
		private static TObject[] SingleArrayCellMap(TObject cell) {
			return cell == null ? null : new TObject[] {cell};
		}

		/// <summary>
		/// Gets the first value of a column.
		/// </summary>
		/// <param name="column"></param>
		/// <returns>
		/// Returns the <see cref="TObject"/> value that represents the first item 
		/// in the set or <b>null</b> if there are no items in the column set.
		/// </returns>
		public TObject GetFirstCellContent(int column) {
			IntegerVector ivec = SelectFirst(column);
			if (ivec.Count > 0)
				return GetCellContents(column, ivec[0]);
			return null;
		}

		/// <summary>
		/// Gets an array of the first values of the given columns.
		/// </summary>
		/// <param name="columnIndices"></param>
		/// <returns>
		/// Returns the <see cref="TObject"/> values that represents the first items 
		/// in the set or <b>null</b> if there are no items in the column set.
		/// </returns>
		public TObject[] GetFirstCellContent(int[] columnIndices) {
			if (columnIndices.Length > 1)
				throw new NotSupportedException("Multi-column GetLastCellContent not supported.");

			return SingleArrayCellMap(GetFirstCellContent(columnIndices[0]));
		}

		/// <summary>
		/// Gets the last value of a column.
		/// </summary>
		/// <param name="column"></param>
		/// <returns>
		/// Returns the TObject value that represents the last item in the set or
		/// null if there are no items in the column set.
		/// </returns>
		public TObject GetLastCellContent(int column) {
			IntegerVector ivec = SelectLast(column);
			return ivec.Count > 0 ? GetCellContents(column, ivec[0]) : null;
		}

		///<summary>
		/// Returns the TObject value that represents the last item in the set or
		/// null if there are no items in the column set.
		///</summary>
		///<param name="columnIndices"></param>
		///<returns></returns>
		///<exception cref="ApplicationException"></exception>
		public TObject[] GetLastCellContent(int[] columnIndices) {
			if (columnIndices.Length > 1)
				throw new NotSupportedException("Multi-column GetLastCellContent not supported.");

			return SingleArrayCellMap(GetLastCellContent(columnIndices[0]));
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
		public TObject GetSingleCellContent(int column) {
			IntegerVector ivec = SelectFirst(column);
			int sz = ivec.Count;
			return sz == RowCount && sz > 0 ? GetCellContents(column, ivec[0]) : null;
		}

		///<summary>
		/// If the given column contains all items of the same value, this 
		/// method returns the value.
		///</summary>
		///<param name="col_map"></param>
		///<returns></returns>
		/// <remarks>
		/// If it doesn't, or the column set is empty it returns null.
		/// </remarks>
		///<exception cref="ApplicationException"></exception>
		public TObject[] GetSingleCellContent(int[] col_map) {
			if (col_map.Length > 1)
				throw new NotSupportedException("Multi-column GetSingleCellContent not supported.");

			return SingleArrayCellMap(GetSingleCellContent(col_map[0]));
		}

		/// <summary>
		/// Checks if the given column contains the given value.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="cell"></param>
		/// <returns>
		/// Returns <b>true</b> if the given value is found in the table
		/// for the given column, otherwise <b>false</b>.
		/// </returns>
		public bool ColumnContainsCell(int column, TObject cell) {
			IntegerVector ivec = SelectRows(column, Operator.Get("="), cell);
			return ivec.Count > 0;
		}

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
		public static bool CompareCells(TObject ob1, TObject ob2, Operator op) {
			TObject result = op.Evaluate(ob1, ob2, null, null, null);
			// NOTE: This will be a NullPointerException if the result is not a
			//   boolean type.
			bool? bresult = result.ToNullableBoolean();
			if (!bresult.HasValue)
				throw new NullReferenceException();
			return bresult.Value;
		}

		/// <summary>
		/// Converts the table to a <see cref="IDictionary"/>.
		/// </summary>
		/// <returns>
		/// Returns the table as a <see cref="IDictionary"/>
		/// with the key/pair set.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If the table has more or less then two columns or if the first 
		/// column is not a string column.
		/// </exception>
		public IDictionary<string, object> ToDictionary() {
			if (ColumnCount != 2)
				throw new ApplicationException("Table must have two columns.");

			Dictionary<string, object> map = new Dictionary<string, object>();
			IRowEnumerator en = GetRowEnumerator();
			while (en.MoveNext()) {
				int rowIndex = en.RowIndex;
				TObject key = GetCellContents(0, rowIndex);
				TObject value = GetCellContents(1, rowIndex);
				map[key.Object.ToString()] = value.Object;
			}

			return map;
		}

		/// <summary>
		/// Provides faster way to find a column index given a column name.
		/// </summary>
		/// <param name="col">Name of the column to get the index for.</param>
		/// <returns>
		/// Returns the index of the column for the given name, or -1
		/// if not found.
		/// </returns>
		public int FastFindFieldName(VariableName col) {
			lock (columnLookupLock) {
				if (columnNameLookup == null)
					columnNameLookup = new Dictionary<VariableName, int>(30);

				int index;
				if (!columnNameLookup.TryGetValue(col, out index)) {
					index = FindFieldName(col);
					columnNameLookup[col] = index;
				}

				return index;
			}
		}

		/// <summary>
		/// Returns a TableVariableResolver object for this table.
		/// </summary>
		/// <returns></returns>
		internal TableVariableResolver GetVariableResolver() {
			return new TableVariableResolver(this);
		}


		// ---------- Inner classes ----------

		/// <inheritdoc/>
		public override String ToString() {
			String name = "VT" + GetHashCode();
			if (this is DataTableBase) {
				name = ((DataTableBase) this).TableName.ToString();
			}
			return name + "[" + RowCount + "]";
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
				if (colIndex == -1)
					throw new ApplicationException("Can't find column: " + variable);
				return colIndex;
			}

			public int SetId {
				get { return rowIndex; }
				set { rowIndex = value; }
			}

			public TObject Resolve(VariableName variable) {
				return table.GetCellContents(FindColumnName(variable), rowIndex);
			}

			public TType ReturnTType(VariableName variable) {
				return table.GetTTypeForColumn(variable);
			}
		}
	}
}