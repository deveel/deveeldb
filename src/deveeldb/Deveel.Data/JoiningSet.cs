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
using System.Collections.Generic;

namespace Deveel.Data {
	/// <summary>
	/// Used in a table set to describe how we naturally join the tables 
	/// together.
	/// </summary>
	/// <remarks>
	/// This is used when the table set has evaluated the search condition 
	/// and it is required for any straggling tables to be naturally joined.
	/// In SQL, these joining types are specified in the <c>FROM</c> clause.
	/// </remarks>
	/// <example>
	/// <code>
	/// FROM table_a LEFT OUTER JOIN table_b ON ( table_a.id = table_b.id ), ...
	/// </code>
	/// A ',' should donate an <c>INNER JOIN</c> in an SQL <c>FROM</c> clause.
	/// </example>
	[Serializable]
	public sealed class JoiningSet : ICloneable {
		/// <summary>
		/// The list of tables we are joining together a JoinPart object that
		/// represents how the tables are joined.
		/// </summary>
		private List<object> joinSet;

		///<summary>
		///</summary>
		public JoiningSet() {
			joinSet = new List<object>();
		}

		/// <summary>
		/// Resolves the schema of tables in this joining set.
		/// </summary>
		/// <param name="connection"></param>
		/// <remarks>
		/// This runs through each table in the joining set and if the 
		/// schema has not been set for the table then it attempts to 
		/// resolve it against the given <paramref name="connection"/>. 
		/// This would typically be called in the preparation of a statement.
		/// </remarks>
		public void Prepare(DatabaseConnection connection) {
		}

		/// <summary>
		/// Adds a new table into the set being joined.
		/// </summary>
		/// <param name="tableName">The name of the table to add.</param>
		/// <remarks>
		/// The table name should be the unique name that distinguishes 
		/// this table in the table set.
		/// </remarks>
		public void AddTable(TableName tableName) {
			joinSet.Add(tableName);
		}

		/// <summary>
		/// Add a joining type to the previous entry from the end.
		/// </summary>
		/// <param name="type"></param>
		/// <param name="onExpression"></param>
		/// <remarks>
		/// This is an artifact of how joins are parsed.
		/// </remarks>
		public void AddPreviousJoin(JoinType type, Expression onExpression) {
			joinSet.Insert(joinSet.Count - 1, new JoinPart(type, onExpression));
		}

		/// <summary>
		/// Adds a joining type to the set with a given <i>on</i> expression.
		/// </summary>
		/// <param name="type"></param>
		/// <param name="on_expression"></param>
		public void AddJoin(JoinType type, Expression on_expression) {
			joinSet.Add(new JoinPart(type, on_expression));
		}

		/// <summary>
		/// Adds a joining type to the set.
		/// </summary>
		/// <param name="type">Join type to add to the set.</param>
		public void AddJoin(JoinType type) {
			joinSet.Add(new JoinPart(type));
		}

		/// <summary>
		/// Gets the number of tables that are in this set.
		/// </summary>
		public int TableCount {
			get { return (joinSet.Count + 1)/2; }
		}

		/// <summary>
		/// Returns the first table in the join set.
		/// </summary>
		public TableName FirstTable {
			get { return this[0]; }
		}

		/// <summary>
		/// Gets or sets the name of the table at the given index.
		/// </summary>
		/// <param name="n">The zero-based index of the table to get.</param>
		/// <returns>
		/// Returns a <see cref="TableName"/> of the table at the given 
		/// <paramref name="n"/> of the set.
		/// </returns>
		public TableName this[int n] {
			get { return (TableName) joinSet[n*2]; }
		}

		/// <summary>
		/// Sets the table at the given position in this joining set.
		/// </summary>
		/// <param name="n"></param>
		/// <param name="table"></param>
		private void SetTable(int n, TableName table) {
			joinSet[n * 2] = table;
		}

		/// <summary>
		/// Gets the join type after the given index of a table.
		/// </summary>
		/// <param name="n">Index of the table to get the subsequent 
		/// join type.</param>
		/// <example>
		/// <code lang="C#">
		/// TableName table1 = joins.FirstTable;
		/// int i = 0;
		/// while(i &lt; joins.TableCount - 1) {
		///		JoinType type = joins.GetJoinType(i);
		///		TableName table2 = GetTable(i + 1);
		///		// ... Join table1 and table2 ...
		///		table1 = table2;
		///		i++;
		/// }
		/// </code>
		/// </example>
		/// <returns>
		/// </returns>
		public JoinType GetJoinType(int n) {
			return ((JoinPart)joinSet[(n * 2) + 1]).type;
		}

		/// <summary>
		/// Gets the join <i>on</i> expression after the given index of a table.
		/// </summary>
		/// <param name="n">Index of the table to get the subsequent 
		/// join type.</param>
		public Expression GetOnExpression(int n) {
			return ((JoinPart)joinSet[(n * 2) + 1]).on_expression;
		}

		/// <inheritdoc/>
		public object Clone() {
			JoiningSet v = (JoiningSet)MemberwiseClone();
			int size = joinSet.Count;
			List<object> clonedJoinSet = new List<object>(size);
			v.joinSet = clonedJoinSet;

			for (int i = 0; i < size; ++i) {
				Object element = joinSet[i];
				if (element is TableName) {
					// immutable so leave alone
				} else if (element is JoinPart) {
					element = ((JoinPart)element).Clone();
				} else {
					throw new ApplicationException(element.GetType().ToString());
				}

				clonedJoinSet.Add(element);
			}

			return v;
		}



		// ---------- Inner classes ----------
		[Serializable]
		private sealed class JoinPart : ICloneable {
			/// <summary>
			/// The type of join.  Either Left,
			/// Right, Full, Inner.
			/// </summary>
			public readonly JoinType type;

			/// <summary>
			/// The expression that we are joining on (eg. ON clause in SQL).  If there
			/// is no ON expression (such as in the case of natural joins) then this is
			/// null.
			/// </summary>
			public Expression on_expression;

			public JoinPart(JoinType type, Expression on_expression) {
				this.type = type;
				this.on_expression = on_expression;
			}

			public JoinPart(JoinType type)
				: this(type, null) {
			}

			/// <inheritdoc/>
			public Object Clone() {
				JoinPart v = (JoinPart)MemberwiseClone();
				if (on_expression != null)
					v.on_expression = (Expression)on_expression.Clone();
				return v;
			}
		}
	}
}