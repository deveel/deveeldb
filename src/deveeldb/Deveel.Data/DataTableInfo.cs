// 
//  Copyright 2010-2011  Deveel
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

namespace Deveel.Data {
	/// <summary>
	/// Defines meta informations about a table.
	/// </summary>
	/// <remarks>
	/// Every table in the database has a definition that describes how it is stored 
	/// on disk, the column definitions, primary keys/foreign keys, and any 
	/// check constraints.
	/// </remarks>
	public sealed class DataTableInfo : ICloneable {
		/// <summary>
		///  A TableName object that represents this data table info.
		/// </summary>
		private TableName tableName;

		/// <summary>
		/// The list of DataTableColumnInfo objects that are the definitions of each
		/// column input the table.
		/// </summary>
		private List<DataTableColumnInfo> columnList;

		/// <summary>
		/// Set to true if this data table info is immutable.
		/// </summary>
		private bool immutable;

		///<summary>
		///</summary>
		public DataTableInfo() {
			columnList = new List<DataTableColumnInfo>();
			immutable = false;
		}

		///<summary>
		/// Sets this DataTableInfo to immutable which means nothing is 
		/// able to change it.
		///</summary>
		public void SetImmutable() {
			immutable = true;
		}

		///<summary>
		/// Returns true if this is immutable.
		///</summary>
		public bool IsImmutable {
			get { return immutable; }
		}

		/// <summary>
		/// Checks that this object is mutable.
		/// </summary>
		/// <exception cref="ApplicationException">
		/// If the current <see cref="DataTableInfo"/> is immutable.
		/// </exception>
		private void CheckMutable() {
			if (IsImmutable)
				throw new ApplicationException("Tried to mutate immutable object.");
		}


		/// <summary>
		/// Resolves variables input a column so that any unresolved column 
		/// names point to this table.
		/// </summary>
		/// <param name="ignoreCase"></param>
		/// <param name="exp"></param>
		/// <remarks>
		/// Used to resolve columns input the 'check_expression'.
		/// </remarks>
		internal void ResolveColumns(bool ignoreCase, Expression exp) {
			// For each variable, determine if the column correctly resolves to a
			// column input this table.  If the database is input identifier case insensitive
			// mode attempt to resolve the column name to a valid column input this
			// info.
			if (exp != null) {
				IList<VariableName> list = exp.AllVariables;
				foreach (VariableName v in list) {
					string colName = v.Name;
					// Can we resolve this to a variable input the table?
					if (ignoreCase) {
						int size = ColumnCount;
						for (int n = 0; n < size; ++n) {
							// If this is a column name (case ignored) then set the variable
							// to the correct cased name.
							if (String.Compare(this[n].Name, colName, true) == 0) {
								v.Name = this[n].Name;
							}
						}
					}
				}
			}
		}

		/// <summary>
		/// Resolves a single column name to its correct form.
		/// </summary>
		/// <param name="columnName">Column name to resolve</param>
		/// <param name="ignoreCase"><b>true</b> if must resolve in case insensitive 
		/// mode, otherwise <b>false</b>.</param>
		/// <remarks>
		/// For example, if the database is in case insensitive mode it will 
		/// resolve ID to 'id' if 'id' is in this table.
		/// </remarks>
		/// <returns>
		/// Returns the properly resolved column name for <paramref name="columnName"/>.
		/// </returns>
		/// <exception cref="System.ArgumentNullException">If <paramref name="columnName"/> is <b>null</b>
		/// or zero-length.</exception>
		/// <exception cref="DatabaseException">
		/// If <paramref name="columnName"/> couldn't be resolved (ambiguous 
		/// or not found).
		/// </exception>
		public string ResolveColumnName(string columnName, bool ignoreCase) {
			// Can we resolve this to a column input the table?
			string found = null;
			foreach (DataTableColumnInfo columnDef in columnList) {
				// If this is a column name (case ignored) then set the column
				// to the correct cased name.
				string this_col_name = columnDef.Name;
				if (String.Compare(this_col_name, columnName, ignoreCase) == 0) {
					if (!String.IsNullOrEmpty(found))
						throw new DatabaseException("Ambiguous reference to column '" + columnName + "'");

					found = this_col_name;
				}
			}

			if (found == null)
				throw new DatabaseException("Column '" + columnName + "' not found");

			return found;
		}

		/// <summary>
		/// Given a list of column names referencing entries in this table, this will
		/// resolve each one to its correct form.
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="list"></param>
		/// <exception cref="System.ArgumentNullException">If any column name of the list 
		/// is <b>null</b> or zero-length, or if <paramref name="connection"/> is <b>null</b></exception>
		/// <exception cref="DatabaseException">If any column name of the list couldn't
		/// be resolved (ambiguous or not found).</exception>
		public void ResolveColumnsInList(DatabaseConnection connection, IList<string> list) {
			bool ignoreCase = connection.IsInCaseInsensitiveMode;
			for (int i = 0; i < list.Count; ++i) {
				list[i] = ResolveColumnName(list[i], ignoreCase);
			}
		}

		///<summary>
		///</summary>
		///<param name="columnInfo"></param>
		///<exception cref="ApplicationException"></exception>
		public void AddColumn(DataTableColumnInfo columnInfo) {
			CheckMutable();
			// Is there already a column with this name input the table info?
			foreach (DataTableColumnInfo cd in columnList) {
				if (cd.Name.Equals(columnInfo.Name))
					throw new ApplicationException("Duplicated columns found.");
			}

			columnList.Add(columnInfo);
		}

		///<summary>
		///</summary>
		///<param name="columnInfo"></param>
		/// <remarks>
		/// Same as <see cref="AddColumn"/> only this does not perform a 
		/// check to ensure no two columns are the same.
		/// </remarks>
		public void AddVirtualColumn(DataTableColumnInfo columnInfo) {
			CheckMutable();
			columnList.Add(columnInfo);
		}

		/// <summary>
		/// Gets the name of the schema the table belongs to if any,
		/// otherwise returns <see cref="String.Empty"/>.
		/// </summary>
		public string Schema {
			get { return tableName.Schema ?? String.Empty; }
		}

		/// <summary>
		/// Gets the name of the table.
		/// </summary>
		public string Name {
			get { return tableName.Name; }
		}

		/// <summary>
		/// Gets the <see cref="TableName"/> object representing the full name 
		/// of the table.
		/// </summary>
		public TableName TableName {
			get { return tableName; }
			set { tableName = value; }
		}


		/// <summary>
		/// Gets the number of columns in the table.
		/// </summary>
		public int ColumnCount {
			get { return columnList.Count; }
		}

		/// <summary>
		/// Gets the <see cref="DataTableColumnInfo"/> object representing the 
		/// column at the given index.
		/// </summary>
		/// <param name="column">Index of the coulmn to get.</param>
		/// <returns>
		/// Returns a <see cref="DataTableColumnInfo"/> at the given 
		/// <paramref name="column"/> within the table.
		/// </returns>
		/// <exception cref="System.IndexOutOfRangeException">
		/// If <paramref name="column"/> is out of range of the column list.
		/// </exception>
		public DataTableColumnInfo this[int column] {
			get { return columnList[column]; }
		}

		///<summary>
		///</summary>
		///<param name="columnName"></param>
		///<returns></returns>
		public int FindColumnName(string columnName) {
			int size = ColumnCount;
			for (int i = 0; i < size; ++i) {
				if (this[i].Name.Equals(columnName)) {
					return i;
				}
			}
			return -1;
		}

		private Dictionary<string, int> colNameLookup;
		private readonly object colLookupLock = new object();

		///<summary>
		/// A faster way to find a column index given a string column name.
		///</summary>
		///<param name="columnName"></param>
		/// <remarks>
		/// This caches column name -> column index input a hashtable.
		/// </remarks>
		///<returns></returns>
		public int FastFindColumnName(string columnName) {
			lock (colLookupLock) {
				if (colNameLookup == null)
					colNameLookup = new Dictionary<string, int>(30);

				int index;
				if (!colNameLookup.TryGetValue(columnName, out index)) {
					index = FindColumnName(columnName);
					colNameLookup[columnName] = index;
				}

				return index;
			}
		}


		/// <summary>
		/// Copies the object, excluding the columns and the constraints
		/// contained in it.
		/// </summary>
		/// <returns></returns>
		public DataTableInfo NoColumnCopy() {
			DataTableInfo info = new DataTableInfo();
			info.TableName = TableName;
			return info;
		}


		// ---------- In/Out methods ----------

		/// <summary>
		/// Writes this DataTableInfo file to the data output stream.
		/// </summary>
		/// <param name="output"></param>
		internal void Write(BinaryWriter output) {
			output.Write(2);  // Version number

			output.Write(Name);
			output.Write(Schema);            // Added input version 2
			output.Write(columnList.Count);
			foreach (DataTableColumnInfo columnDef in columnList) {
				columnDef.Write(output);
			}
		}

		/// <summary>
		/// Reads this DataTableInfo file from the data input stream.
		/// </summary>
		/// <param name="input"></param>
		/// <returns></returns>
		internal static DataTableInfo Read(BinaryReader input) {
			DataTableInfo dtf = new DataTableInfo();
			int ver = input.ReadInt32();
			if (ver == 1)
				throw new NotSupportedException("Version 1 DataTableInfo no longer supported.");
			if (ver != 2)
				throw new ApplicationException("Unrecognized DataTableInfo version (" + ver + ")");

			string rname = input.ReadString();
			string rschema = input.ReadString();
			dtf.TableName = new TableName(rschema, rname);
			int size = input.ReadInt32();
			for (int i = 0; i < size; ++i) {
				DataTableColumnInfo colInfo = DataTableColumnInfo.Read(input);
				dtf.columnList.Add(colInfo);
			}

			dtf.SetImmutable();
			return dtf;
		}

		object ICloneable.Clone() {
			return Clone();
		}

		public DataTableInfo Clone() {
			DataTableInfo tableInfo = new DataTableInfo();
			tableInfo.tableName = tableName;
			tableInfo.columnList = new List<DataTableColumnInfo>();
			foreach (DataTableColumnInfo columnDef in columnList) {
				tableInfo.columnList.Add(columnDef.Clone());
			}
			tableInfo.immutable = false;
			return tableInfo;
		}
	}
}