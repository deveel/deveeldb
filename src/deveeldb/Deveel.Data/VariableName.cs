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

using Deveel.Data.DbSystem;

namespace Deveel.Data {
	/// <summary>
	/// This represents a column name that may be qualified.
	/// </summary>
	/// <remarks>
	/// This object encapsulated a column name that can be fully 
	/// qualified in the system.  Such uses of this object would 
	/// not typically be used against any context.  For example, 
	/// it would not be desirable to use <i>ColumnName</i> in 
	/// <see cref="DataTableInfo"/> because the column names contained 
	/// in <see cref="DataTableInfo"/> are within a known context.  
	/// This object is intended for use within parser processes 
	/// where free standing column names with potentially no context 
	/// are required.
	/// <para>
	/// <b>Note</b>: This object is <b>not</b> immutable.
	/// </para>
	/// </remarks>
	[Serializable]
	public sealed class VariableName : ICloneable, IComparable {
		/// <summary>
		/// Static that represents an unknown table name.
		/// </summary>
		private static readonly TableName UnknownTableName = new TableName("##UNKNOWN_TABLE_NAME##");

		/// <summary>
		/// The TableName that is the context of this column.  This may be
		/// <see cref="UnknownTableName"/> if the table name is not known.
		/// </summary>
		private TableName table_name;

		/// <summary>
		/// The column name itself.
		/// </summary>
		private String column_name;

		public VariableName(TableName table_name, String column_name) {
			if (table_name == null || column_name == null) {
				throw new ArgumentNullException();
			}
			this.table_name = table_name;
			this.column_name = column_name;
		}

		public VariableName(String column_name)
			: this(UnknownTableName, column_name) {
		}

		public VariableName(VariableName v) {
			table_name = v.table_name;
			column_name = v.column_name;
		}

		/// <summary>
		/// Returns the <see cref="TableName"/> context.
		/// </summary>
		public TableName TableName {
			get { return !(table_name.Equals(UnknownTableName)) ? table_name : null; }
			set {
				if (value == null)
					throw new ArgumentNullException("value");
				table_name = value;
			}
		}


		/// <summary>
		/// Gets or sets the column name context.
		/// </summary>
		/// <remarks>
		/// Setting the name of the column should be done if the variable 
		/// is resolved from one form to another.
		/// </remarks>
		public string Name {
			get { return column_name; }
			set {
				if (value == null)
					throw new ArgumentNullException("value");
				column_name = value;
			}
		}

		/// <summary>
		/// Attempts to resolve a string '[table_name].[column]' to a 
		/// <see cref="VariableName"/> instance.
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		public static VariableName Resolve(String name) {
			int div = name.LastIndexOf(".");
			if (div != -1) {
				// Column represents '[something].[name]'
				String column_name = name.Substring(div + 1);
				// Make the '[something]' into a TableName
				TableName table_name = TableName.Resolve(name.Substring(0, div));
				// Set the variable name
				return new VariableName(table_name, column_name);
			}
			// Column represents '[something]'
			return new VariableName(name);
		}

		/// <summary>
		/// Attempts to resolve a string '[table_name].[column]' to a 
		/// <see cref="VariableName"/> instance.
		/// </summary>
		/// <param name="tname"></param>
		/// <param name="name"></param>
		/// <remarks>
		/// If the table name does not exist, or the table name schema is 
		/// not specified, then the schema/table name is copied from the 
		/// given object.
		/// </remarks>
		/// <returns></returns>
		public static VariableName Resolve(TableName tname, String name) {
			VariableName v = Resolve(name);
			return v.TableName == null
			       	? new VariableName(tname, v.Name)
			       	: (v.TableName.Schema == null ? new VariableName(new TableName(tname.Schema, v.TableName.Name), v.Name) : v);
		}


		/// <summary>
		/// Returns a <see cref="VariableName"/> that is resolved against a 
		/// table name context only if the <see cref="VariableName"/> is 
		/// unknown in this object.
		/// </summary>
		/// <param name="tablen"></param>
		/// <returns></returns>
		public VariableName ResolveTableName(TableName tablen) {
			return table_name.Equals(UnknownTableName)
			       	? new VariableName(tablen, Name)
			       	: new VariableName(table_name.ResolveSchema(tablen.Schema), Name);
		}

		/// <summary>
		/// Sets this <see cref="VariableName"/> object with information 
		/// from the given <see cref="VariableName"/>.
		/// </summary>
		/// <param name="from"></param>
		/// <returns></returns>
		public VariableName Set(VariableName from) {
			table_name = from.table_name;
			column_name = from.column_name;
			return this;
		}


		// ----

		/// <inheritdoc/>
		public Object Clone() {
			return MemberwiseClone();
		}

		/// <inheritdoc/>
		public override String ToString() {
			return TableName != null ? TableName + "." + Name : Name;
		}

		public String ToTechString() {
			TableName tn = TableName;
			if (tn != null) {
				return tn.Schema + "^" + tn.Name + "^" + Name;
			}
			return Name;
		}

		/// <inheritdoc/>
		public override bool Equals(Object ob) {
			VariableName cn = (VariableName)ob;
			return cn.table_name.Equals(table_name) &&
				   cn.column_name.Equals(column_name);
		}

		/// <inheritdoc/>
		public int CompareTo(Object ob) {
			VariableName cn = (VariableName)ob;
			int v = table_name.CompareTo(cn.table_name);
			return v == 0 ? column_name.CompareTo(cn.column_name) : v;
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return table_name.GetHashCode() + column_name.GetHashCode();
		}
	}
}