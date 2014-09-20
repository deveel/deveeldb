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
using System.Text;

namespace Deveel.Data.Sql {
	/// <summary>
	/// A container for the <i>FROM</i> clause of a select statement.
	/// </summary>
	/// <remarks>
	/// This handles the different types of joins.
	/// </remarks>
	[Serializable]
	public sealed class FromClause : IStatementTreeObject {
		internal FromClause() {
		}

		/// <summary>
		/// The JoiningSet object that we have created to represent the joins 
		/// in this <c>FROM</c> clause.
		/// </summary>
		private JoiningSet joinSet = new JoiningSet();

		/// <summary>
		/// A list of all <see cref="FromTable"/> objects in this clause in 
		/// order of when they were specified.
		/// </summary>
		private List<FromTable> fromTableList = new List<FromTable>();

		/// <summary>
		/// A list of all table names in this from clause.
		/// </summary>
		private List<string> allTableNames = new List<string>();

		/// <summary>
		/// An id used for making unique names for anonymous inner selects.
		/// </summary>
		private int tableKey;


		/// <summary>
		/// Creates a new unique key string.
		/// </summary>
		/// <returns></returns>
		private String CreateNewKey() {
			++tableKey;
			return tableKey.ToString();
		}


		private void AddFromTable(string tableName, FromTable table) {
			if (tableName != null) {
				if (allTableNames.Contains(tableName))
					throw new ApplicationException("Duplicate table name in FROM clause: " + tableName);

				allTableNames.Add(tableName);
			}

			// Create a new unique key for this table
			string key = CreateNewKey();
			table.UniqueKey = key;
			// Add the table key to the join set
			joinSet.AddTable(new TableName(key));
			// Add to the alias def map
			fromTableList.Add(table);
		}

		/// <summary>
		/// Adds a table name to this FROM clause.
		/// </summary>
		/// <param name="tableName">The name of the table to add to the 
		/// clause.</param>
		/// <remarks>
		/// The given name may be a dot deliminated reference such 
		/// as (schema.table_name).
		/// </remarks>
		public void AddTable(string tableName) {
			AddFromTable(tableName, new FromTable(tableName));
		}

		/// <summary>
		/// Adds a table name and its alias to the clause.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="tableAlias"></param>
		public void AddTable(string tableName, string tableAlias) {
			AddFromTable(tableAlias, new FromTable(tableName, tableAlias));
		}

		/// <summary>
		/// A generic form of a table declaration.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="select"></param>
		/// <param name="tableAlias"></param>
		/// <remarks>
		/// If any parameters are <b>null</b> it means the information is 
		/// not available.
		/// </remarks>
		public void AddTableDeclaration(string tableName, TableSelectExpression select, string tableAlias) {
			// This is an inner select in the FROM clause
			if (tableName == null && select != null) {
				if (tableAlias == null) {
					AddFromTable(null, new FromTable(select));
				} else {
					AddFromTable(tableAlias, new FromTable(select, tableAlias));
				}
			}
				// This is a standard table reference in the FROM clause
			else if (tableName != null && select == null) {
				if (tableAlias == null) {
					AddTable(tableName);
				} else {
					AddTable(tableName, tableAlias);
				}
			}
				// Error
			else {
				throw new ApplicationException("Unvalid declaration parameters.");
			}
		}

		/// <summary>
		/// Adds a Join to the from clause.
		/// </summary>
		/// <param name="type"></param>
		public void AddJoin(JoinType type) {
			joinSet.AddJoin(type);
		}

		///<summary>
		/// Add a joining type to the previous entry from the end.
		///</summary>
		///<param name="type"></param>
		///<param name="onExpression"></param>
		/// <remarks>
		/// This is an artifact of how joins are parsed.
		/// </remarks>
		public void AddPreviousJoin(JoinType type, Expression onExpression) {
			joinSet.AddPreviousJoin(type, onExpression);
		}

		///<summary>
		/// Adds a Join to the from clause.
		///</summary>
		///<param name="type">The type of the join.</param>
		///<param name="on_expression">The expression representing the <c>ON</c>
		/// condition.</param>
		public void AddJoin(JoinType type, Expression on_expression) {
			joinSet.AddJoin(type, on_expression);
		}

		/// <summary>
		/// Returns the JoiningSet object for the FROM clause.
		/// </summary>
		public JoiningSet JoinSet {
			get { return joinSet; }
		}

		/// <summary>
		/// Returns the type of join after table 'n' in the set of tables 
		/// in the from clause.
		/// </summary>
		/// <param name="n"></param>
		/// <returns></returns>
		public JoinType GetJoinType(int n) {
			return JoinSet.GetJoinType(n);
		}

		///<summary>
		/// Returns the <c>ON</c> <see cref="Expression"/> for the type of join 
		/// after table <paramref name="n"/> in the set.
		///</summary>
		///<param name="n"></param>
		///<returns></returns>
		public Expression GetOnExpression(int n) {
			return JoinSet.GetOnExpression(n);
		}

		///<summary>
		/// Returns a <see cref="ICollection">collection</see> of <see cref="FromTable"/> 
		/// objects that represent all the tables that are in this from clause.
		///</summary>
		public ICollection<FromTable> AllTables {
			get { return fromTableList.AsReadOnly(); }
		}

		/// <inheritdoc/>
		void IStatementTreeObject.PrepareExpressions(IExpressionPreparer preparer) {
			// Prepare expressions in the JoiningSet first
			int size = joinSet.TableCount - 1;
			for (int i = 0; i < size; ++i) {
				Expression exp = joinSet.GetOnExpression(i);
				if (exp != null) {
					exp.Prepare(preparer);
				}
			}
			// Prepare the StatementTree sub-queries in the from tables
			foreach (FromTable table in fromTableList) {
				table.PrepareExpressions(preparer);
			}
		}

		/// <inheritdoc/>
		public object Clone() {
			FromClause v = (FromClause)MemberwiseClone();
			v.joinSet = (JoiningSet)joinSet.Clone();

			v.allTableNames = new List<string>();
			foreach (string tableName in allTableNames) {
				v.allTableNames.Add((string)tableName.Clone());
			}

			v.fromTableList = new List<FromTable>(fromTableList.Count);
			foreach (FromTable table in fromTableList) {
				v.fromTableList.Add((FromTable)table.Clone());
			}

			return v;
		}

		internal void DumpTo(StringBuilder sb) {
			//TODO:
		}
	}
}