// 
//  Copyright 2010-2016 Deveel
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
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;

namespace Deveel.Data.Sql.Expressions {
	/// <summary>
	/// A container for the <i>FROM</i> clause of a select statement.
	/// </summary>
	/// <remarks>
	/// This handles the different types of joins.
	/// </remarks>
	[Serializable]
	public sealed class FromClause : IPreparable, ISerializable, ISqlFormattable {
		public FromClause() {
			fromTables = new List<FromTable>();
			joinParts = new List<JoinPart>();
			tableNames = new List<string>();
		}

		private FromClause(SerializationInfo info, StreamingContext context) {
			var tableNames = (string[])info.GetValue("TableNames", typeof(string[]));
			var fromTables = (FromTable[]) info.GetValue("FromTables", typeof(FromTable[]));
			var joinParts = (JoinPart[])info.GetValue("JoinParts", typeof(JoinPart[]));

			this.tableNames = new List<string>();
			this.fromTables = new List<FromTable>();
			this.joinParts = new List<JoinPart>();

			if (tableNames != null)
				this.tableNames.AddRange(tableNames);
			if (fromTables != null)
				this.fromTables.AddRange(fromTables);
			if (joinParts != null)
				this.joinParts.AddRange(joinParts);
		}

		private readonly List<string> tableNames;
		private readonly List<FromTable> fromTables;
		private readonly List<JoinPart> joinParts;

		/// <summary>
		/// An id used for making unique names for anonymous inner selects.
		/// </summary>
		private int tableKey;

		/// <summary>
		/// Gets an enumeration of all the tables that are the source of the query.
		/// </summary>
		public IEnumerable<FromTable> AllTables {
			get { return fromTables.ToArray(); }
		}

		/// <summary>
		/// Gets a count of all the joins happening in the clause.
		/// </summary>
		public int JoinPartCount {
			get { return joinParts.Count; }
		}

		public bool IsEmpty {
			get { return fromTables.Count == 0; }
		}

		private String CreateNewKey() {
			++tableKey;
			return tableKey.ToString(CultureInfo.InvariantCulture);
		}

		/// <summary>
		/// Adds a table as source to the query with a given alias.
		/// </summary>
		/// <param name="alias">The unique name alias of the table source within the clause.</param>
		/// <param name="table">The table source object to query from.</param>
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

		/// <summary>
		/// Adds a simple table reference as the source of the query with a given alias.
		/// </summary>
		/// <param name="alias">The unique name alias of the table source within the clause.</param>
		/// <param name="tableName">The name of the table in the database to query from.</param>
		/// <seealso cref="AddTable(string, FromTable)"/>
		public void AddTable(string alias, string tableName) {
			AddTable(alias, new FromTable(tableName, alias));
		}

		/// <summary>
		/// Adds a simple table reference as the source of the query.
		/// </summary>
		/// <param name="tableName">The name of the table in the database to query from.</param>
		/// <seealso cref="AddTable(string, string)"/>
		public void AddTable(string tableName) {
			AddTable(null, tableName);
		}

		/// <summary>
		/// Adds a sub-query expression as source of the query.
		/// </summary>
		/// <param name="subQuery">The sub-query expression as source of the query.</param>
		/// <seealso cref="AddSubQuery(string, SqlQueryExpression)"/>
		public void AddSubQuery(SqlQueryExpression subQuery) {
			AddSubQuery(null, subQuery);
		}

		/// <summary>
		/// Adds a sub-query expression as source of the query.
		/// </summary>
		/// <param name="alias">The unique alias name of the expression within the clause.</param>
		/// <param name="subQuery">The sub-query expression as source of the query.</param>
		/// <seealso cref="AddTable(string, FromTable)"/>
		/// <seealso cref="SqlQueryExpression"/>
		public void AddSubQuery(string alias, SqlQueryExpression subQuery) {
			AddTable(alias, new FromTable(subQuery, alias));
		}

		/// <summary>
		/// Sets a join between the last added table and the one that preceeds it.
		/// </summary>
		/// <param name="joinType">The type of join to apply to the two tables.</param>
		/// <param name="onExpression">The condition for the two tables to join.</param>
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

		/// <summary>
		/// Gets the descriptor of the join at the given offset.
		/// </summary>
		/// <param name="offset">The offset of the join descriptor to get.</param>
		/// <returns>
		/// Returns an instance of <seealso cref="JoinPart"/> that describes the type
		/// of join at the given offset within the clause.
		/// </returns>
		/// <seealso cref="Join"/>
		public JoinPart GetJoinPart(int offset) {
			return joinParts[offset];
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var clause = new FromClause();

			// Prepare expressions in the JoiningSet first
			int size = joinParts.Count;
			for (int i = 0; i < size; ++i) {
				var part = joinParts[i];
				var exp = part.OnExpression;
				if (exp != null) {
					exp = exp.Prepare(preparer);
					if (part.SubQuery != null) {
						part = new JoinPart(part.JoinType, part.SubQuery,  exp);
					} else {
						part = new JoinPart(part.JoinType, part.TableName, exp);
					}
				}

				clause.joinParts.Add(part);
			}

			// Prepare the StatementTree sub-queries in the from tables
			for (int i = 0; i < fromTables.Count; i++) {
				var table = fromTables[i];
				var preparedTable = (FromTable) ((IPreparable) table).Prepare(preparer);

				if (i < tableNames.Count) {
					var tableAlias = tableNames[i];
					clause.tableNames.Insert(i, tableAlias);
				}
				
				clause.fromTables.Insert(i, preparedTable);
			}

			return clause;
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			if (tableNames != null)
				info.AddValue("TableNames", tableNames.ToArray());
			if (fromTables != null)
				info.AddValue("FromTables", fromTables.ToArray());
			if (joinParts != null)
				info.AddValue("JoinParts", joinParts.ToArray());
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			if (IsEmpty)
				return;

			builder.Append("FROM ");

			var tables = AllTables.ToList();
			for (int i = 0; i < tables.Count; i++) {
				var source = tables[i];

				JoinPart joinPart = null;

				if (i > 0) {
					joinPart = GetJoinPart(i - 1);
					if (joinPart != null &&
						joinPart.OnExpression != null) {
						if (joinPart.JoinType == JoinType.Inner) {
							builder.Append(" INNER JOIN ");
						} else if (joinPart.JoinType == JoinType.Right) {
							builder.Append(" RIGHT OUTER JOIN ");
						} else if (joinPart.JoinType == JoinType.Left) {
							builder.Append(" LEFT OUTER JOIN ");
						} else if (joinPart.JoinType == JoinType.Full) {
							builder.Append(" FULL OUTER JOINT ");
						}
					}
				}

				if (i > 0 &&
					(joinPart == null || joinPart.OnExpression == null))
					builder.Append(", ");

				if (source.IsSubQuery) {
					builder.Append("(");
					source.SubQuery.AppendTo(builder);
					builder.Append(")");
				} else {
					builder.Append(source.Name);
				}

				if (!String.IsNullOrEmpty(source.Alias)) {
					builder.Append(" AS ");
					builder.Append(source.Alias);
				}

				if (joinPart != null &&
					joinPart.OnExpression != null) {
					builder.Append(" ON ");
					joinPart.OnExpression.AppendTo(builder);
				}
			}
		}

		public override string ToString() {
			var builder = new SqlStringBuilder();
			this.AppendTo(builder);
			return builder.ToString();
		}
	}
}