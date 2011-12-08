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

using Deveel.Data.Collections;
using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// A simple interface for querying a <see cref="ITableDataSource"/>
	/// instance.
	/// </summary>
	/// <remarks>
	/// This is used as a very lightweight interface for changing a table.
	/// It is most useful for internal low level users of a database table 
	/// which doesn't need the overhead of the table hierarchy mechanism.
	/// </remarks>
	public sealed class SimpleTableQuery : IDisposable {

		/// <summary>
		/// The DataTableDef for this table.
		/// </summary>
		private readonly DataTableDef table_def;

		/// <summary>
		/// The ITableDataSource we are wrapping.
		/// </summary>
		private ITableDataSource table;

		~SimpleTableQuery() {
			Dispose(false);
		}

		void IDisposable.Dispose() {
			GC.SuppressFinalize(this);
			Dispose(true);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				Dispose();
			}
		}

		/// <summary>
		/// Constructs the <see cref="SimpleTableQuery"/> with the given 
		/// <see cref="IMutableTableDataSource"/> object.
		/// </summary>
		/// <param name="in_table"></param>
		public SimpleTableQuery(ITableDataSource in_table) {
			//    in_table.AddRootLock();
			table = in_table;
			table_def = table.TableInfo;
		}

		/// <summary>
		/// Gets an enumeration of rows for the underlying table.
		/// </summary>
		/// <returns>
		/// Returns a <see cref="IRowEnumerator"/> used to iterate through the 
		/// entire list of valid rows in the table.
		/// </returns>
		public IRowEnumerator GetRowEnumerator() {
			return table.GetRowEnumerator();
		}

		/// <summary>
		/// Returns the total number of rows in the underlying table.
		/// </summary>
		public int RowCount {
			get { return table.RowCount; }
		}

		///<summary>
		/// Gets the TObject at the given cell in the table.
		///</summary>
		///<param name="column"></param>
		///<param name="row"></param>
		/// <remarks>
		/// The offset between one valid row and the next may not necessily be 1.  
		/// It is possible for there to be gaps in the data.  For an iterator that 
		/// returns successive row indexes, use the <see cref="GetRowEnumerator"/> method.
		/// </remarks>
		///<returns></returns>
		public TObject Get(int column, int row) {
			return table.GetCellContents(column, row);
		}

		/// <summary>
		/// Finds the index of all the rows in the table where the given column is
		/// equal to the given value.
		/// </summary>
		/// <param name="column">Index of the column to select.</param>
		/// <param name="cell">Value to compare for the selection.</param>
		/// <returns>
		/// Returns a list of row indices (as <see cref="IntegerVector"/>)
		/// from the underlying table which equal to the given <paramref name="cell"/>.
		/// </returns>
		public IntegerVector SelectEqual(int column, TObject cell) {
			return table.GetColumnScheme(column).SelectEqual(cell);
		}

		/// <summary>
		/// Finds the index of all the rows in the table where the given column is
		/// equal to the given value.
		/// </summary>
		/// <param name="column">Index of the column to select.</param>
		/// <param name="value">Value to compare for the selection.</param>
		/// <remarks>
		/// We assume value is not null, and it is either a <see cref="BigNumber"/> 
		/// to represent a number, a <see cref="String"/>, a <see cref="DateTime"/> 
		/// or a <see cref="ByteLongObject"/>.
		/// </remarks>
		/// <returns>
		/// Returns a list of row indices (as <see cref="IntegerVector"/>)
		/// from the underlying table which equal to the given <paramref name="value"/>.
		/// </returns>
		public IntegerVector SelectEqual(int column, Object value) {
			TType ttype = table_def[column].TType;
			TObject cell = new TObject(ttype, value);
			return SelectEqual(column, cell);
		}

		/// <summary>
		/// Finds the index of all the rows in the table where the given column is
		/// equal to the given object for both of the clauses.
		/// </summary>
		/// <param name="col1">First column index of the clause.</param>
		/// <param name="cell1">Value to compare to the first column  of the clause.</param>
		/// <param name="col2">Second column index of the clause.</param>
		/// <param name="cell2">Value to compare to the second column  of the clause.</param>
		/// <returns>
		/// Returns a list of row indices (as <see cref="IntegerVector"/>)
		/// from the underlying table which equal to the given caluse.
		/// </returns>
		public IntegerVector SelectEqual(int col1, TObject cell1,
												int col2, TObject cell2) {

			// All the indexes that equal the first clause
			IntegerVector ivec = table.GetColumnScheme(col1).SelectEqual(cell1);

			// From this, remove all the indexes that don't equals the second clause.
			int index = ivec.Count - 1;
			while (index >= 0) {
				// If the value in column 2 at this index is not equal to value then
				// remove it from the list and move to the next.
				if (Get(col2, ivec[index]).CompareTo(cell2) != 0) {
					ivec.RemoveIntAt(index);
				}
				--index;
			}

			return ivec;
		}

		/// <summary>
		/// Finds the index of all the rows in the table where the given column is
		/// equal to the given object for both of the clauses.
		/// </summary>
		/// <param name="col1">First column index of the clause.</param>
		/// <param name="val1">Value to compare to the first column  of the clause.</param>
		/// <param name="col2">Second column index of the clause.</param>
		/// <param name="val2">Value to compare to the second column  of the clause.</param>
		/// <remarks>
		/// We assume value is not null, and it is either a <see cref="BigNumber"/> 
		/// to represent a number, a <see cref="String"/>, a <see cref="DateTime"/> 
		/// or a <see cref="ByteLongObject"/>.
		/// </remarks>
		/// <returns>
		/// Returns a list of row indices (as <see cref="IntegerVector"/>)
		/// from the underlying table which equal to the given caluse.
		/// </returns>
		public IntegerVector SelectEqual(int col1, Object val1,
												int col2, Object val2) {
			TType t1 = table_def[col1].TType;
			TType t2 = table_def[col2].TType;

			TObject cell1 = new TObject(t1, val1);
			TObject cell2 = new TObject(t2, val2);

			return SelectEqual(col1, cell1, col2, cell2);
		}

		/// <summary>
		/// Check if there is a single row in the table where the given column
		/// is equal to the given value.
		/// </summary>
		/// <param name="col">The index of the coulmn to check.</param>
		/// <param name="val">The value to compare.</param>
		/// <returns>
		/// Returns <b>true</b> if there is a single row in the table where the given column
		/// is equal to the given value, otherwise returns <b>false</b>.
		/// </returns>
		/// <exception cref="ApplicationException">
		/// If multiple rows were found.
		/// </exception>
		public bool Exists(int col, Object val) {
			IntegerVector ivec = SelectEqual(col, val);
			if (ivec.Count == 0) {
				return false;
			} else if (ivec.Count == 1) {
				return true;
			} else {
				throw new ApplicationException("Assertion failed: Exists found multiple values.");
			}
		}

		/// <summary>
		/// Assuming the table stores a key/value mapping, this returns the contents
		/// of <paramref name="value_column"/> for any rows where <paramref name="key_column"/>
		/// is equal to the <paramref name="key_value"/>.
		/// </summary>
		/// <param name="value_column"></param>
		/// <param name="key_column"></param>
		/// <param name="key_value"></param>
		/// <returns>
		/// Returns the value of <paramref name="value_column"/> if found, otherwise
		/// returns <b>null</b>.
		/// </returns>
		/// <exception cref="ApplicationException">
		/// If there is more than one row that match the key.
		/// </exception>
		public Object GetVariable(int value_column, int key_column, Object key_value) {
			// All indexes in the table where the key value is found.
			IntegerVector ivec = SelectEqual(key_column, key_value);
			if (ivec.Count > 1) {
				throw new ApplicationException("Assertion failed: GetVariable found multiple key values.");
			} else if (ivec.Count == 0) {
				// Key found so return the value
				return Get(value_column, ivec[0]);
			} else {
				// Key not found so return null
				return null;
			}
		}

		// ---------- Table mutable methods ---------

		/// <summary>
		/// Adds a new key/value mapping in the underlying table.
		/// </summary>
		/// <param name="key_column">Index of the column containing the key value.</param>
		/// <param name="vals">Array of values to insert to the underlying table.</param>
		/// <remarks>
		/// If the key already exists the old key/value row is deleted first.
		/// <para>
		/// The <paramref name="vals"/> array must be the size of the number 
		/// of columns in the underlying tbale.
		/// </para>
		/// <para>
		/// <b>Notices:</b>
		/// <list type="bullet">
		/// <item>Change will come into effect globally at the next commit.</item>
		/// <item>This method must be assured of exclusive access to the table within
		/// the transaction.</item>
		/// <item>This only works if the given table implements 
		/// <see cref="IMutableTableDataSource"/>.</item>
		/// </list>
		/// </para>
		/// </remarks>
		/// <exception cref="InvalidOperationException">
		/// If the underlying table is not a <see cref="IMutableTableDataSource"/>.
		/// </exception>
		/// <exception cref="ApplicationException">
		/// If multiple values were found for <paramref name="key_column"/>.
		/// </exception>
		public void SetVariable(int key_column, Object[] vals) {
			// Cast to a IMutableTableDataSource
			IMutableTableDataSource mtable = (IMutableTableDataSource)table;

			// All indexes in the table where the key value is found.
			IntegerVector ivec = SelectEqual(key_column, vals[key_column]);
			if (ivec.Count > 1) {
				throw new ApplicationException("Assertion failed: SetVariable found multiple key values.");
			} else if (ivec.Count == 1) {
				// Remove the current key
				mtable.RemoveRow(ivec[0]);
			}
			// Insert the new key
			DataRow dataRow = new DataRow(table);
			for (int i = 0; i < table_def.ColumnCount; ++i) {
				dataRow.SetValue(i, vals[i]);
			}
			mtable.AddRow(dataRow);
		}

		/// <summary>
		/// Deletes a single entry from the table where the given column equals the
		/// given value.
		/// </summary>
		/// <param name="col"></param>
		/// <param name="val"></param>
		/// <returns>
		/// Returns <b>true</b> if a single value was found and deleted, otherwise <b>false</b>.
		/// </returns>
		/// <exception cref="InvalidOperationException">
		/// If the underlying table is not a <see cref="IMutableTableDataSource"/>.
		/// </exception>
		/// <exception cref="ApplicationException">
		/// If multiple values were found.
		/// </exception>
		public bool Delete(int col, Object val) {
			// Cast to a IMutableTableDataSource
			IMutableTableDataSource mtable = (IMutableTableDataSource)table;

			IntegerVector ivec = SelectEqual(col, val);
			if (ivec.Count == 0) {
				return false;
			} else if (ivec.Count == 1) {
				mtable.RemoveRow(ivec[0]);
				return true;
			} else {
				throw new ApplicationException("Assertion failed: Delete found multiple values.");
			}
		}

		/// <summary>
		/// Deletes all the given indexes in this table.
		/// </summary>
		/// <param name="list"></param>
		/// <remarks>
		/// This only works if the given table implements <see cref="IMutableTableDataSource"/>.
		/// </remarks>
		/// <exception cref="InvalidOperationException">
		/// If the underlying table is not a <see cref="IMutableTableDataSource"/>.
		/// </exception>
		public void DeleteRows(IntegerVector list) {
			// Cast to a IMutableTableDataSource
			IMutableTableDataSource mtable = (IMutableTableDataSource)table;

			for (int i = 0; i < list.Count; ++i) {
				mtable.RemoveRow(list[i]);
			}
		}

		/// <inheritdoc/>
		public void Dispose() {
			if (table != null) {
				//      table.RemoveRootLock();
				table = null;
			}
		}
	}
}