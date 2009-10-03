// 
//  FromTableDef.cs
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

namespace Deveel.Data.Sql {
	/// <summary>
	/// Describes a single table declaration in the from clause of a 
	/// table expression (<c>SELECT</c>).
	/// </summary>
	[Serializable]
	public sealed class FromTableDef : ICloneable {
		/// <summary>
		/// If this is true, then the table def represents a sub-query table.
		/// </summary>
		/// <remarks>
		/// The <see cref="FromTableDef.TableSelectExpression"/> and <see cref="Alias"/> 
		/// method can be used to get the table information.
		/// </remarks>
		/// <example>
		/// <code>
		/// FROM ( SELECT id, number FROM Part ) AS part_info, ....
		/// </code>
		/// </example>
		private readonly bool subquery_table;

		/// <summary>
		/// The unique key name given to this table definition.
		/// </summary>
		private String unique_key;

		/// <summary>
		/// The name of the table this definition references.
		/// </summary>
		private readonly String table_name;

		/// <summary>
		/// The alias of the table or null if no alias was defined.
		/// </summary>
		private readonly String table_alias;

		/// <summary>
		/// The TableSelectExpression if this is a subquery table.
		/// </summary>
		private TableSelectExpression subselect_table;

		/// <summary>
		/// Constructs a table that is aliased under a different name.
		/// </summary>
		/// <param name="table_name"></param>
		/// <param name="table_alias"></param>
		public FromTableDef(String table_name, String table_alias) {
			this.table_name = table_name;
			this.table_alias = table_alias;
			subselect_table = null;
			subquery_table = false;
		}

		/// <summary>
		/// A simple table definition (not aliased).
		/// </summary>
		/// <param name="table_name"></param>
		public FromTableDef(String table_name)
			: this(table_name, null) {
		}

		/// <summary>
		/// A table that is a sub-query and given an aliased name.
		/// </summary>
		/// <param name="select"></param>
		/// <param name="table_alias"></param>
		public FromTableDef(TableSelectExpression select, String table_alias) {
			subselect_table = select;
			table_name = table_alias;
			this.table_alias = table_alias;
			subquery_table = true;
		}

		/// <summary>
		/// A simple sub-query table definition (not aliased).
		/// </summary>
		/// <param name="select"></param>
		public FromTableDef(TableSelectExpression select) {
			subselect_table = select;
			table_name = null;
			table_alias = null;
			subquery_table = true;
		}


		///<summary>
		/// Gets the name of the table.
		///</summary>
		public string Name {
			get { return table_name; }
		}

		/// <summary>
		/// Returns the alias for this table (or null if no alias given).
		/// </summary>
		public string Alias {
			get { return table_alias; }
		}

		/// <summary>
		/// Gets or sets the unique key.
		/// </summary>
		public string UniqueKey {
			get { return unique_key; }
			set { unique_key = value; }
		}

		/// <summary>
		/// Returns true if this item in the FROM clause is a subquery table.
		/// </summary>
		public bool IsSubQueryTable {
			get { return subquery_table; }
		}

		/// <summary>
		/// Returns the TableSelectExpression if this is a subquery table.
		/// </summary>
		public TableSelectExpression TableSelectExpression {
			get { return subselect_table; }
		}

		///<summary>
		/// Prepares the expressions in this table def.
		///</summary>
		///<param name="preparer"></param>
		public void PrepareExpressions(IExpressionPreparer preparer) {
			if (subselect_table != null) {
				subselect_table.PrepareExpressions(preparer);
			}
		}

		/// <inheritdoc/>
		public Object Clone() {
			FromTableDef v = (FromTableDef)MemberwiseClone();
			if (subselect_table != null) {
				v.subselect_table = (TableSelectExpression)subselect_table.Clone();
			}
			return v;
		}

	}
}