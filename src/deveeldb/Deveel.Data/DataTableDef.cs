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
	public class DataTableDef {
		/// <summary>
		///  A TableName object that represents this data table def.
		/// </summary>
		private TableName table_name;

		/// <summary>
		/// The type of table this is (this is the class name of the object that
		/// maintains the underlying database files).
		/// </summary>
		private String table_type_class;

		/// <summary>
		/// The list of DataTableColumnDef objects that are the definitions of each
		/// column input the table.
		/// </summary>
		private readonly ArrayList column_list;

		/// <summary>
		/// Set to true if this data table def is immutable.
		/// </summary>
		private bool immutable;

		///<summary>
		///</summary>
		public DataTableDef() {
			column_list = new ArrayList();
			table_type_class = "";
			immutable = false;
		}

		///<summary>
		///</summary>
		///<param name="table_def"></param>
		public DataTableDef(DataTableDef table_def) {
			table_name = table_def.TableName;
			table_type_class = table_def.table_type_class;
			column_list = (ArrayList)table_def.column_list.Clone();

			// Copy is not immutable
			immutable = false;
		}

		///<summary>
		/// Sets this DataTableDef to immutable which means nothing is 
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
		/// If the current <see cref="DataTableDef"/> is immutable.
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
			// def.
			if (exp != null) {
				IList list = exp.AllVariables;
				for (int i = 0; i < list.Count; ++i) {
					VariableName v = (VariableName)list[i];
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
		///<param name="col_def"></param>
		///<exception cref="ApplicationException"></exception>
		public void AddColumn(DataTableColumnDef col_def) {
			CheckMutable();
			// Is there already a column with this name input the table def?
			for (int i = 0; i < column_list.Count; ++i) {
				DataTableColumnDef cd = (DataTableColumnDef)column_list[i];
				if (cd.Name.Equals(col_def.Name)) {
					throw new ApplicationException("Duplicated columns found.");
				}
			}
			column_list.Add(col_def);
		}

		///<summary>
		///</summary>
		///<param name="col_def"></param>
		/// <remarks>
		/// Same as <see cref="AddColumn"/> only this does not perform a 
		/// check to ensure no two columns are the same.
		/// </remarks>
		public void AddVirtualColumn(DataTableColumnDef col_def) {
			CheckMutable();
			column_list.Add(col_def);
		}

		/// <summary>
		/// Gets the name of the schema the table belongs to if any,
		/// otherwise returns <see cref="String.Empty"/>.
		/// </summary>
		public string Schema {
			get {
				String schema_name = table_name.Schema;
				return schema_name == null ? "" : schema_name;
			}
		}

		/// <summary>
		/// Gets the name of the table.
		/// </summary>
		public string Name {
			get { return table_name.Name; }
		}

		/// <summary>
		/// Gets the <see cref="TableName"/> object representing the full name 
		/// of the table.
		/// </summary>
		public TableName TableName {
			get { return table_name; }
			set { table_name = value; }
		}

		/// <summary>
		/// Gets or sets the type string of the table.
		/// </summary>
		/// <exception cref="DatabaseException">
		/// If the specified value is invalid.</exception>
		public string TableType {
			get { return table_type_class; }
			set {
				CheckMutable();
				if (value.Equals("Deveel.Data.VariableSizeDataTableFile")) {
					table_type_class = value;
				} else {
					throw new ApplicationException("Unrecognised table class: " + value);
				}
			}
		}

		/// <summary>
		/// Gets the number of columns in the table.
		/// </summary>
		public int ColumnCount {
			get { return column_list.Count; }
		}

		/// <summary>
		/// Gets the <see cref="DataTableColumnDef"/> object representing the 
		/// column at the given index.
		/// </summary>
		/// <param name="column">Index of the coulmn to get.</param>
		/// <returns>
		/// Returns a <see cref="DataTableColumnDef"/> at the given 
		/// <paramref name="column"/> within the table.
		/// </returns>
		/// <exception cref="System.IndexOutOfRangeException">
		/// If <paramref name="column"/> is out of range of the column list.
		/// </exception>
		public DataTableColumnDef this[int column] {
			get { return (DataTableColumnDef) column_list[column]; }
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
		public DataTableDef NoColumnCopy() {
			DataTableDef def = new DataTableDef();
			def.TableName = TableName;
			//    def.setSchema(schema);
			//    def.setName(name);

			def.table_type_class = table_type_class;

			return def;
		}


		// ---------- In/Out methods ----------

		/// <summary>
		/// Writes this DataTableDef file to the data output stream.
		/// </summary>
		/// <param name="output"></param>
		internal void Write(BinaryWriter output) {
			output.Write(2);  // Version number

			output.Write(Name);
			output.Write(Schema);            // Added input version 2
			output.Write(table_type_class);
			output.Write(column_list.Count);
			for (int i = 0; i < column_list.Count; ++i) {
				((DataTableColumnDef)column_list[i]).Write(output);
			}

			//    // -- Added input version 2 --
			//    // Write the constraint list.
			//    output.writeInt(constraint_list.size());
			//    for (int i = 0; i < constraint_list.size(); ++i) {
			//      ((DataTableConstraintDef) constraint_list.get(i)).Write(output);
			//    }

			//    [ this is removed from version 1 ]
			//    if (check_expression != null) {
			//      output.writeBoolean(true);
			//      // Write the text version of the expression to the stream.
			//      output.writeUTF(new String(check_expression.text()));
			//    }
			//    else {
			//      output.writeBoolean(false);
			//    }

		}

		/// <summary>
		/// Reads this DataTableDef file from the data input stream.
		/// </summary>
		/// <param name="input"></param>
		/// <returns></returns>
		internal static DataTableDef Read(BinaryReader input) {
			DataTableDef dtf = new DataTableDef();
			int ver = input.ReadInt32();
			if (ver == 1)
				throw new IOException("Version 1 DataTableDef no longer supported.");
			if (ver == 2) {
				String rname = input.ReadString();
				String rschema = input.ReadString();
				dtf.TableName = new TableName(rschema, rname);
				dtf.table_type_class = input.ReadString();
				int size = input.ReadInt32();
				for (int i = 0; i < size; ++i) {
					DataTableColumnDef col_def = DataTableColumnDef.Read(input);
					dtf.column_list.Add(col_def);
				}

			} else {
				throw new ApplicationException("Unrecognized DataTableDef version (" + ver + ")");
			}

			dtf.SetImmutable();
			return dtf;
		}
	}
}