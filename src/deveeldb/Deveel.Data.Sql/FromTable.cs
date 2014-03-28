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

namespace Deveel.Data.Sql {
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
		/// The <see cref="SubSelect"/> and <see cref="Alias"/> 
		/// method can be used to get the table information.
		/// </remarks>
		/// <example>
		/// <code>
		/// FROM ( SELECT id, number FROM Part ) AS part_info, ....
		/// </code>
		/// </example>
		private readonly bool subqueryTable;

		/// <summary>
		/// The unique key name given to this table definition.
		/// </summary>
		private string uniqueKey;

		/// <summary>
		/// The name of the table this definition references.
		/// </summary>
		private readonly string tableName;

		/// <summary>
		/// The alias of the table or null if no alias was defined.
		/// </summary>
		private readonly string tableAlias;

		/// <summary>
		/// The TableSelectExpression if this is a subquery table.
		/// </summary>
		private TableSelectExpression subselectTable;

		/// <summary>
		/// Constructs a table that is aliased under a different name.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="tableAlias"></param>
		public FromTable(string tableName, string tableAlias) {
			this.tableName = tableName;
			this.tableAlias = tableAlias;
			subselectTable = null;
			subqueryTable = false;
		}

		/// <summary>
		/// A simple table definition (not aliased).
		/// </summary>
		/// <param name="tableName"></param>
		public FromTable(string tableName)
			: this(tableName, null) {
		}

		/// <summary>
		/// A table that is a sub-query and given an aliased name.
		/// </summary>
		/// <param name="select"></param>
		/// <param name="tableAlias"></param>
		public FromTable(TableSelectExpression select, string tableAlias) {
			subselectTable = select;
			tableName = tableAlias;
			this.tableAlias = tableAlias;
			subqueryTable = true;
		}

		/// <summary>
		/// A simple sub-query table definition (not aliased).
		/// </summary>
		/// <param name="select"></param>
		public FromTable(TableSelectExpression select) {
			subselectTable = select;
			tableName = null;
			tableAlias = null;
			subqueryTable = true;
		}


		///<summary>
		/// Gets the name of the table.
		///</summary>
		public string Name {
			get { return tableName; }
		}

		/// <summary>
		/// Returns the alias for this table (or null if no alias given).
		/// </summary>
		public string Alias {
			get { return tableAlias; }
		}

		/// <summary>
		/// Gets or sets the unique key.
		/// </summary>
		internal string UniqueKey {
			get { return uniqueKey; }
			set { uniqueKey = value; }
		}

		/// <summary>
		/// Returns true if this item in the FROM clause is a subquery table.
		/// </summary>
		public bool IsSubQueryTable {
			get { return subqueryTable; }
		}

		/// <summary>
		/// Returns the TableSelectExpression if this is a subquery table.
		/// </summary>
		public TableSelectExpression SubSelect {
			get { return subselectTable; }
		}

		///<summary>
		/// Prepares the expressions in this table def.
		///</summary>
		///<param name="preparer"></param>
		internal void PrepareExpressions(IExpressionPreparer preparer) {
			if (subselectTable != null)
				((IStatementTreeObject) subselectTable).PrepareExpressions(preparer);
		}

		/// <inheritdoc/>
		public object Clone() {
			FromTable v = (FromTable)MemberwiseClone();
			if (subselectTable != null) {
				v.subselectTable = (TableSelectExpression)subselectTable.Clone();
			}
			return v;
		}
	}
}