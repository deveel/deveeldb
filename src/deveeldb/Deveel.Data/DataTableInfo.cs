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

namespace Deveel.Data {
	/// <summary>
	/// Defines meta information about a table.
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
		private bool immutable;

		///<summary>
		///</summary>
		public DataTableInfo() {
			columns = new List<DataTableColumnInfo>();
			tableTypeName = "";
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
			if (IsImmutable) {
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
		/// <param name="ignore_case"></param>
		/// <param name="exp"></param>
		/// <remarks>
		/// Used to resolve columns input the 'check_expression'.
		/// </remarks>
		internal void ResolveColumns(bool ignore_case, Expression exp) {

			// For each variable, determine if the column correctly resolves to a
			// column input this table.  If the database is input identifier case insensitive
			// mode attempt to resolve the column name to a valid column input this
			// info.
			if (exp != null) {
				IList<VariableName> list = exp.AllVariables;
				for (int i = 0; i < list.Count; ++i) {
					VariableName v = list[i];
					String col_name = v.Name;
					// Can we resolve this to a variable input the table?
					if (ignore_case) {
						int size = ColumnCount;
						for (int n = 0; n < size; ++n) {
							// If this is a column name (case ignored) then set the variable
							// to the correct cased name.
							if (String.Compare(this[n].Name, col_name, true) == 0) {
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
		/// <param name="col_name">Column name to resolve</param>
		/// <param name="ignore_case"><b>true</b> if must resolve in case insensitive 
		/// mode, otherwise <b>false</b>.</param>
		/// <remarks>
		/// For example, if the database is in case insensitive mode it will 
		/// resolve ID to 'id' if 'id' is in this table.
		/// </remarks>
		/// <returns>
		/// Returns the properly resolved column name for <paramref name="col_name"/>.
		/// </returns>
		/// <exception cref="System.ArgumentNullException">If <paramref name="col_name"/> is <b>null</b>
		/// or zero-length.</exception>
		/// <exception cref="DatabaseException">
		/// If <paramref name="col_name"/> couldn't be resolved (ambiguous 
		/// or not found).
		/// </exception>
		public String ResolveColumnName(String col_name, bool ignore_case) {
			// Can we resolve this to a column input the table?
			int size = ColumnCount;
			int found = -1;
			for (int n = 0; n < size; ++n) {
				// If this is a column name (case ignored) then set the column
				// to the correct cased name.
				String this_col_name = this[n].Name;
				if (ignore_case && String.Compare(this_col_name, col_name, true) == 0) {
					if (found == -1) {
						found = n;
					} else {
						throw new DatabaseException(
									"Ambiguous reference to column '" + col_name + "'");
					}
				} else if (!ignore_case && this_col_name.Equals(col_name)) {
					found = n;
				}
			}
			if (found != -1) {
				return this[found].Name;
			} else {
				throw new DatabaseException("Column '" + col_name + "' not found");
			}
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
		public void ResolveColumnsInArray(DatabaseConnection connection, ArrayList list) {
			bool ignore_case = connection.IsInCaseInsensitiveMode;
			for (int i = 0; i < list.Count; ++i) {
				String col_name = (String)list[i];
				list[i] = ResolveColumnName((String)list[i], ignore_case);
			}
		}

		// ---------- Set methods ----------

		///<summary>
		///</summary>
		///<param name="colInfo"></param>
		///<exception cref="ApplicationException"></exception>
		public void AddColumn(DataTableColumnInfo colInfo) {
			CheckMutable();
			// Is there already a column with this name input the table info?
			for (int i = 0; i < columns.Count; ++i) {
				DataTableColumnInfo cd = (DataTableColumnInfo)columns[i];
				if (cd.Name.Equals(colInfo.Name)) {
					throw new ApplicationException("Duplicated columns found.");
				}
			}
			columns.Add(colInfo);
		}

		///<summary>
		///</summary>
		///<param name="colInfo"></param>
		/// <remarks>
		/// Same as <see cref="AddColumn"/> only this does not perform a 
		/// check to ensure no two columns are the same.
		/// </remarks>
		public void AddVirtualColumn(DataTableColumnInfo colInfo) {
			CheckMutable();
			columns.Add(colInfo);
		}

		/// <summary>
		/// Gets the name of the schema the table belongs to if any,
		/// otherwise returns <see cref="String.Empty"/>.
		/// </summary>
		public string Schema {
			get {
				String schema_name = tableName.Schema;
				return schema_name == null ? "" : schema_name;
			}
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
					throw new ApplicationException("Unrecognised table class: " + value);
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
			get { return (DataTableColumnInfo) columns[column]; }
		}

		///<summary>
		///</summary>
		///<param name="column_name"></param>
		///<returns></returns>
		public int FindColumnName(String column_name) {
			int size = ColumnCount;
			for (int i = 0; i < size; ++i) {
				if (this[i].Name.Equals(column_name)) {
					return i;
				}
			}
			return -1;
		}

		// Stores col name -> col index lookups
		private Hashtable col_name_lookup;
		private Object COL_LOOKUP_LOCK = new Object();

		///<summary>
		/// A faster way to find a column index given a string column name.
		///</summary>
		///<param name="col"></param>
		/// <remarks>
		/// This caches column name -> column index input a hashtable.
		/// </remarks>
		///<returns></returns>
		public int FastFindColumnName(String col) {
			lock (COL_LOOKUP_LOCK) {
				if (col_name_lookup == null) {
					col_name_lookup = new Hashtable(30);
				}
				Object ob = col_name_lookup[col];
				if (ob == null) {
					int ci = FindColumnName(col);
					col_name_lookup[col] =ci;
					return ci;
				} else {
					return (int)ob;
				}
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
			//    info.setSchema(schema);
			//    info.setName(name);

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
			DataTableInfo tableInfo = new DataTableInfo();
			int ver = input.ReadInt32();
			if (ver == 1)
				throw new IOException("Version 1 DataTableInfo no longer supported.");

			if (ver == 2) {
				string rname = input.ReadString();
				string rschema = input.ReadString();
				tableInfo.TableName = new TableName(rschema, rname);
				tableInfo.tableTypeName = input.ReadString();
				int size = input.ReadInt32();
				for (int i = 0; i < size; ++i) {
					DataTableColumnInfo colInfo = DataTableColumnInfo.Read(input);
					tableInfo.columns.Add(colInfo);
				}

			} else {
				throw new ApplicationException("Unrecognized DataTableInfo version (" + ver + ")");
			}

			tableInfo.SetImmutable();
			return tableInfo;
		}

		object ICloneable.Clone() {
			return Clone();
		}

		public DataTableInfo Clone() {
			DataTableInfo clone = new DataTableInfo();
			clone.tableName = tableName;
			clone.tableTypeName = (string) tableTypeName.Clone();
			clone.columns = new List<DataTableColumnInfo>();
			foreach (DataTableColumnInfo column in columns) {
				clone.columns.Add(column.Clone());
			}

			return clone;
		}
	}
}