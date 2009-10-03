// 
//  Variable.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

namespace Deveel.Data {
	/// <summary>
	/// This represents a column name that may be qualified.
	/// </summary>
	/// <remarks>
	/// This object encapsulated a column name that can be fully 
	/// qualified in the system.  Such uses of this object would 
	/// not typically be used against any context.  For example, 
	/// it would not be desirable to use <i>ColumnName</i> in 
	/// <see cref="DataTableDef"/> because the column names contained 
	/// in <see cref="DataTableDef"/> are within a known context.  
	/// This object is intended for use within parser processes 
	/// where free standing column names with potentially no context 
	/// are required.
	/// <para>
	/// <b>Note</b>: This object is <b>not</b> immutable.
	/// </para>
	/// </remarks>
	[Serializable]
	public sealed class Variable : ICloneable, IComparable {
		/// <summary>
		/// Static that represents an unknown table name.
		/// </summary>
		private static readonly TableName UnknownTableName =
										   new TableName("##UNKNOWN_TABLE_NAME##");

		/// <summary>
		/// The TableName that is the context of this column.  This may be
		/// <see cref="UnknownTableName"/> if the table name is not known.
		/// </summary>
		private TableName table_name;

		/// <summary>
		/// The column name itself.
		/// </summary>
		private String column_name;

		public Variable(TableName table_name, String column_name) {
			if (table_name == null || column_name == null) {
				throw new ArgumentNullException();
			}
			this.table_name = table_name;
			this.column_name = column_name;
		}

		public Variable(String column_name)
			: this(UnknownTableName, column_name) {
		}

		public Variable(Variable v) {
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
		/// <see cref="Variable"/> instance.
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		public static Variable Resolve(String name) {
			int div = name.LastIndexOf(".");
			if (div != -1) {
				// Column represents '[something].[name]'
				String column_name = name.Substring(div + 1);
				// Make the '[something]' into a TableName
				TableName table_name = TableName.Resolve(name.Substring(0, div));
				// Set the variable name
				return new Variable(table_name, column_name);
			}
			// Column represents '[something]'
			return new Variable(name);
		}

		/// <summary>
		/// Attempts to resolve a string '[table_name].[column]' to a 
		/// <see cref="Variable"/> instance.
		/// </summary>
		/// <param name="tname"></param>
		/// <param name="name"></param>
		/// <remarks>
		/// If the table name does not exist, or the table name schema is 
		/// not specified, then the schema/table name is copied from the 
		/// given object.
		/// </remarks>
		/// <returns></returns>
		public static Variable Resolve(TableName tname, String name) {
			Variable v = Resolve(name);
			return v.TableName == null
			       	? new Variable(tname, v.Name)
			       	: (v.TableName.Schema == null ? new Variable(new TableName(tname.Schema, v.TableName.Name), v.Name) : v);
		}


		/// <summary>
		/// Returns a <see cref="Variable"/> that is resolved against a 
		/// table name context only if the <see cref="Variable"/> is 
		/// unknown in this object.
		/// </summary>
		/// <param name="tablen"></param>
		/// <returns></returns>
		public Variable ResolveTableName(TableName tablen) {
			return table_name.Equals(UnknownTableName)
			       	? new Variable(tablen, Name)
			       	: new Variable(table_name.ResolveSchema(tablen.Schema), Name);
		}

		/// <summary>
		/// Sets this <see cref="Variable"/> object with information 
		/// from the given <see cref="Variable"/>.
		/// </summary>
		/// <param name="from"></param>
		/// <returns></returns>
		public Variable Set(Variable from) {
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
			Variable cn = (Variable)ob;
			return cn.table_name.Equals(table_name) &&
				   cn.column_name.Equals(column_name);
		}

		/// <inheritdoc/>
		public int CompareTo(Object ob) {
			Variable cn = (Variable)ob;
			int v = table_name.CompareTo(cn.table_name);
			return v == 0 ? column_name.CompareTo(cn.column_name) : v;
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return table_name.GetHashCode() + column_name.GetHashCode();
		}
	}
}