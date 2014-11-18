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
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql {
	/// <summary>
	/// A container for the <i>FROM</i> clause of a select statement.
	/// </summary>
	/// <remarks>
	/// This handles the different types of joins.
	/// </remarks>
	[Serializable]
	public sealed class FromClause : IPreparable {
		internal FromClause() {
			fromTables = new List<FromTable>();
			joinParts = new List<JoinPart>();
			tableNames = new List<string>();
		}

		private readonly List<string> tableNames;
		private readonly List<FromTable> fromTables;
		private readonly List<JoinPart> joinParts;

		/// <summary>
		/// An id used for making unique names for anonymous inner selects.
		/// </summary>
		private int tableKey;

		public IEnumerable<FromTable> AllTables {
			get { return fromTables.AsReadOnly(); }
		}

		public int JoinPartCount {
			get { return joinParts.Count; }
		}

		private String CreateNewKey() {
			++tableKey;
			return tableKey.ToString(CultureInfo.InvariantCulture);
		}


		public void AddTable(string alias, FromTable table) {
			if (table == null) 
				throw new ArgumentNullException("table");

			if (!String.IsNullOrEmpty(alias)) {
				if (tableNames.Contains(alias))
					throw new ArgumentException(String.Format("Duplicated table name {0} is FROM clause.", alias));

				tableNames.Add(alias);
			}

			// Create a new unique key for this table
			string key = CreateNewKey();
			table.UniqueKey = key;
			fromTables.Add(table);
		}

		public void AddTable(string alias, string tableName) {
			AddTable(alias, new FromTable(tableName, alias));
		}

		public void AddTable(string tableName) {
			AddTable(null, tableName);
		}

		public void AddSubQuery(SqlQueryExpression subQuery) {
			AddSubQuery(null, subQuery);
		}

		public void AddSubQuery(string alias, SqlQueryExpression subQuery) {
			AddTable(alias, new FromTable(subQuery, alias));
		}

		public void Join(JoinType joinType, SqlExpression onExpression) {
			var lastTable = fromTables[fromTables.Count - 1];
			if (lastTable.IsSubQuery) {
				var subQuery = lastTable.SubQuery;
				joinParts.Add(new JoinPart(joinType, subQuery, onExpression));
			} else {
				var tableName = ObjectName.Parse(lastTable.Name);
				joinParts.Add(new JoinPart(joinType, tableName, onExpression));
			}
		}

		public JoinPart GetJoinPart(int offset) {
			return joinParts[offset];
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			throw new NotImplementedException();
		}
	}
}