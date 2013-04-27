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
	/// Defines meta information about a table.
	/// </summary>
	/// <remarks>
	/// Every table in the database has a definition that describes how it is stored 
	/// on disk, the column definitions, primary keys/foreign keys, and any 
	/// check constraints.
	/// </remarks>
	[Serializable]
	public sealed class DataTableInfo : ICloneable {
		/// <summary>
		///  A TableName object that represents this data table info.
		/// </summary>
		private readonly TableName tableName;

		/// <summary>
		/// The type of table this is (this is the class name of the object that
		/// maintains the underlying database files).
		/// </summary>
		private string tableTypeName;

		/// <summary>
		/// The list of DataTableColumnInfo objects that are the definitions of each
		/// column input the table.
		/// </summary>
		private List<DataTableColumnInfo> columns;

		/// <summary>
		/// Set to true if this data table info is immutable.
		/// </summary>
		private bool readOnly;

		///<summary>
		///</summary>
		public DataTableInfo(TableName tableName) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			this.tableName = tableName;
			columns = new List<DataTableColumnInfo>();
			tableTypeName = "";
			readOnly = false;
		}

		public DataTableInfo(string schema, string tableName)
			: this(new TableName(schema, tableName)) {
		}

		public DataTableInfo(string tableName)
			: this(TableName.Resolve(tableName)) {
		}

		///<summary>
		/// Gets or sets a value indicating whether this is immutable or not.
		///</summary>
		public bool IsReadOnly {
			get { return readOnly; }
			set { readOnly = value; }
		}

		/// <summary>
		/// Checks that this object is mutable.
		/// </summary>
		/// <exception cref="ApplicationException">
		/// If the current <see cref="DataTableInfo"/> is immutable.
		/// </exception>
		private void CheckMutable() {
			if (IsReadOnly) {
				throw new ApplicationException("Tried to mutate immutable object.");
			}
		}

		///<summary>
		/// Outputs to the <see cref="TextWriter"/> for debugging.
		///</summary>
		///<param name="output"></param>
		public void Dump(TextWriter output) {
			for (int i = 0; i < ColumnCount; ++i) {
				this[i].Dump(output);
				output.WriteLine();
			}
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
				for (int i = 0; i < list.Count; ++i) {
					VariableName v = list[i];
					string colName = v.Name;

					// Can we resolve this to a variable input the table?
					if (ignoreCase) {
						int size = ColumnCount;
						for (int n = 0; n < size; ++n) {
							// If this is a column name (case ignored) then set the variable
							// to the correct cased name.
							if (String.Compare(this[n].Name, colName, StringComparison.OrdinalIgnoreCase) == 0) {
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
			foreach (DataTableColumnInfo columnInfo in columns) {
				// If this is a column name (case ignored) then set the column
				// to the correct cased name.
				if (String.Compare(columnInfo.Name, columnName, ignoreCase) == 0) {
					if (found != null)
						throw new DatabaseException("Ambiguous reference to column '" + columnName + "'");

					found = columnInfo.Name;
				}
			}

			if (found != null)
				return found;

			throw new DatabaseException("Column '" + columnName + "' not found");
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
		internal void ResolveColumnsInArray(DatabaseConnection connection,IList<string> list) {
			bool ignoreCase = connection.IsInCaseInsensitiveMode;
			for (int i = 0; i < list.Count; ++i) {
				string colName = list[i];
				list[i] = ResolveColumnName(colName, ignoreCase);
			}
		}

		internal void AddColumn(DataTableColumnInfo column) {
			CheckMutable();
			column.TableInfo = this;
			columns.Add(column);
		}

		public DataTableColumnInfo AddColumn(string name, TType type, bool notNull) {
			DataTableColumnInfo column = AddColumn(name, type);
			column.IsNotNull = notNull;
			return column;
		}

		public DataTableColumnInfo AddColumn(string name, TType type) {
			CheckMutable();

			if (name == null)
				throw new ArgumentNullException("name");
			if (type == null)
				throw new ArgumentNullException("type");

			foreach (DataTableColumnInfo column in columns) {
				if (column.Name.Equals(name))
					throw new ArgumentException("Column '" + name + "' already exists in table '" + tableName + "'.");
			}

			DataTableColumnInfo newColumn = new DataTableColumnInfo(this, name, type);
			columns.Add(newColumn);
			return newColumn;
		}

		/// <summary>
		/// Gets the name of the schema the table belongs to if any,
		/// otherwise returns <see cref="String.Empty"/>.
		/// </summary>
		public string Schema {
			get { return tableName.Schema ?? ""; }
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
		}

		/// <summary>
		/// Gets or sets the type string of the table.
		/// </summary>
		/// <exception cref="DatabaseException">
		/// If the specified value is invalid.</exception>
		public string TableType {
			get { return tableTypeName; }
			set {
				CheckMutable();
				if (value.Equals("Deveel.Data.VariableSizeDataTableFile")) {
					tableTypeName = value;
				} else {
					throw new ApplicationException("Unrecognised table type: " + value);
				}
			}
		}

		/// <summary>
		/// Gets the number of columns in the table.
		/// </summary>
		public int ColumnCount {
			get { return columns.Count; }
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
			get { return columns[column]; }
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
		private readonly object colLookupLock = new Object();

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
		public DataTableInfo NoColumnClone() {
			DataTableInfo info = new DataTableInfo(tableName);
			info.tableTypeName = tableTypeName;
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
			output.Write(tableTypeName);
			output.Write(columns.Count);
			for (int i = 0; i < columns.Count; ++i) {
				columns[i].Write(output);
			}
		}

		/// <summary>
		/// Reads this DataTableInfo file from the data input stream.
		/// </summary>
		/// <param name="input"></param>
		/// <returns></returns>
		internal static DataTableInfo Read(BinaryReader input) {
			int ver = input.ReadInt32();
			if (ver == 1)
				throw new IOException("Version 1 DataTableInfo no longer supported.");

			if (ver != 2)
				throw new ApplicationException("Unrecognized DataTableInfo version (" + ver + ")");

			string rname = input.ReadString();
			string rschema = input.ReadString();
			DataTableInfo tableInfo = new DataTableInfo(new TableName(rschema, rname));
			tableInfo.tableTypeName = input.ReadString();
			int size = input.ReadInt32();
			for (int i = 0; i < size; ++i) {
				DataTableColumnInfo colInfo = DataTableColumnInfo.Read(tableInfo, input);
				tableInfo.columns.Add(colInfo);
			}

			tableInfo.IsReadOnly = true;
			return tableInfo;
		}

		object ICloneable.Clone() {
			return Clone();
		}

		public DataTableInfo Clone() {
			return Clone(tableName);
		}

		public DataTableInfo Clone(TableName newTableName) {
			DataTableInfo clone = new DataTableInfo(newTableName);
			clone.tableTypeName = (string)tableTypeName.Clone();
			clone.columns = new List<DataTableColumnInfo>();
			foreach (DataTableColumnInfo column in columns) {
				clone.columns.Add(column.Clone());
			}

			return clone;
		}
	}
}