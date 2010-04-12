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

using Deveel.Data.Sql;

namespace Deveel.Data {
	/// <summary>
	/// Describes a single table declaration in the from clause of a 
	/// table expression (<c>SELECT</c>).
	/// </summary>
	[Serializable]
	public sealed class FromTable : ICloneable {
		/// <summary>
		/// If this is true, then the table def represents a sub-query table.
		/// </summary>
		/// <remarks>
		/// The <see cref="FromTable.TableSelectExpression"/> and <see cref="Alias"/> 
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
		public FromTable(string table_name, string table_alias) {
			this.table_name = table_name;
			this.table_alias = table_alias;
			subselect_table = null;
			subquery_table = false;
		}

		/// <summary>
		/// A simple table definition (not aliased).
		/// </summary>
		/// <param name="table_name"></param>
		public FromTable(string table_name)
			: this(table_name, null) {
		}

		/// <summary>
		/// A table that is a sub-query and given an aliased name.
		/// </summary>
		/// <param name="select"></param>
		/// <param name="table_alias"></param>
		public FromTable(TableSelectExpression select, string table_alias) {
			subselect_table = select;
			table_name = table_alias;
			this.table_alias = table_alias;
			subquery_table = true;
		}

		/// <summary>
		/// A simple sub-query table definition (not aliased).
		/// </summary>
		/// <param name="select"></param>
		public FromTable(TableSelectExpression select) {
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
		internal string UniqueKey {
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
		internal void PrepareExpressions(IExpressionPreparer preparer) {
			if (subselect_table != null)
				((IStatementTreeObject) subselect_table).PrepareExpressions(preparer);
		}

		/// <inheritdoc/>
		public Object Clone() {
			FromTable v = (FromTable)MemberwiseClone();
			if (subselect_table != null) {
				v.subselect_table = (TableSelectExpression)subselect_table.Clone();
			}
			return v;
		}

	}
}