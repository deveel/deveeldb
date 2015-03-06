// 
//  Copyright 2010-2015 Deveel
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
//

using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Describes a single table declaration in the from clause of a 
	/// table expression (<c>SELECT</c>).
	/// </summary>
	[Serializable]
	public sealed class FromTable : IPreparable {
		/// <summary>
		/// Constructs a table that is aliased under a different name.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="tableAlias"></param>
		public FromTable(string tableName, string tableAlias)
			: this(tableName, null, tableAlias) {
			if (String.IsNullOrEmpty(tableName))
				throw new ArgumentNullException("tableName");
		}

		/// <summary>
		/// A simple table definition (not aliased).
		/// </summary>
		/// <param name="tableName"></param>
		public FromTable(string tableName)
			: this(tableName, null) {
		}

		/// <summary>
		/// A table that is a sub-query with no alias set.
		/// </summary>
		/// <param name="query"></param>
		public FromTable(SqlQueryExpression query) 
			: this(query, null) {
		}

		/// <summary>
		/// A table that is a sub-query and given an aliased name.
		/// </summary>
		/// <param name="query"></param>
		/// <param name="tableAlias"></param>
		public FromTable(SqlQueryExpression query, string tableAlias)
			: this(null, query, tableAlias) {
			if (query == null)
				throw new ArgumentNullException("query");
		}

		private FromTable(string tableName, SqlQueryExpression query, string alias) {
			Name = tableName;
			SubQuery = query;
			Alias = alias;
			IsSubQuery = query != null;
		}

		///<summary>
		/// Gets the name of the table.
		///</summary>
		public string Name { get; private set; }

		/// <summary>
		/// Returns the alias for this table (or null if no alias given).
		/// </summary>
		public string Alias { get; private set; }

		/// <summary>
		/// Gets or sets the unique key.
		/// </summary>
		internal string UniqueKey { get; set; }

		/// <summary>
		/// Returns true if this item in the FROM clause is a subquery table.
		/// </summary>
		public bool IsSubQuery { get; private set; }

		/// <summary>
		/// Returns the TableSelectExpression if this is a subquery table.
		/// </summary>
		public SqlQueryExpression SubQuery { get; private set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var subQuery = SubQuery;
			if (subQuery != null)
				subQuery = (SqlQueryExpression) subQuery.Prepare(preparer);

			return new FromTable(Name, subQuery, Alias);
		}
	}
}