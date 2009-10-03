// 
//  FromClause.cs
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
using System.Collections;

namespace Deveel.Data.Sql {
	/// <summary>
	/// A container for the <i>FROM</i> clause of a select statement.
	/// </summary>
	/// <remarks>
	/// This handles the different types of joins.
	/// </remarks>
	[Serializable]
	public sealed class FromClause : IStatementTreeObject {
		/// <summary>
		/// The JoiningSet object that we have created to represent the joins 
		/// in this <c>FROM</c> clause.
		/// </summary>
		private JoiningSet join_set = new JoiningSet();

		/// <summary>
		/// A list of all <see cref="FromTableDef"/> objects in this clause in 
		/// order of when they were specified.
		/// </summary>
		private ArrayList def_list = new ArrayList();

		/// <summary>
		/// A list of all table names in this from clause.
		/// </summary>
		private ArrayList all_table_names = new ArrayList();

		/// <summary>
		/// An id used for making unique names for anonymous inner selects.
		/// </summary>
		private int table_key = 0;


		/// <summary>
		/// Creates a new unique key string.
		/// </summary>
		/// <returns></returns>
		private String CreateNewKey() {
			++table_key;
			return table_key.ToString();
		}


		private void AddTableDef(String table_name, FromTableDef def) {
			if (table_name != null) {
				if (all_table_names.Contains(table_name)) {
					throw new ApplicationException("Duplicate table name in FROM clause: " + table_name);
				}
				all_table_names.Add(table_name);
			}
			// Create a new unique key for this table
			String key = CreateNewKey();
			def.UniqueKey = key;
			// Add the table key to the join set
			join_set.AddTable(new TableName(key));
			// Add to the alias def map
			def_list.Add(def);
		}

		/// <summary>
		/// Adds a table name to this FROM clause.
		/// </summary>
		/// <param name="table_name">The name of the table to add to the 
		/// clause.</param>
		/// <remarks>
		/// The given name may be a dot deliminated reference such 
		/// as (schema.table_name).
		/// </remarks>
		public void AddTable(String table_name) {
			AddTableDef(table_name, new FromTableDef(table_name));
		}

		/// <summary>
		/// Adds a table name and its alias to the clause.
		/// </summary>
		/// <param name="table_name"></param>
		/// <param name="table_alias"></param>
		public void AddTable(String table_name, String table_alias) {
			AddTableDef(table_alias, new FromTableDef(table_name, table_alias));
		}

		/// <summary>
		/// A generic form of a table declaration.
		/// </summary>
		/// <param name="table_name"></param>
		/// <param name="select"></param>
		/// <param name="table_alias"></param>
		/// <remarks>
		/// If any parameters are <b>null</b> it means the information is 
		/// not available.
		/// </remarks>
		public void AddTableDeclaration(String table_name, TableSelectExpression select, String table_alias) {
			// This is an inner select in the FROM clause
			if (table_name == null && select != null) {
				if (table_alias == null) {
					AddTableDef(null, new FromTableDef(select));
				} else {
					AddTableDef(table_alias, new FromTableDef(select, table_alias));
				}
			}
				// This is a standard table reference in the FROM clause
			else if (table_name != null && select == null) {
				if (table_alias == null) {
					AddTable(table_name);
				} else {
					AddTable(table_name, table_alias);
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
		public void addJoin(JoinType type) {
			//    Console.Out.WriteLine("Add Join: " + type);
			join_set.AddJoin(type);
		}

		///<summary>
		/// Add a joining type to the previous entry from the end.
		///</summary>
		///<param name="type"></param>
		///<param name="on_expression"></param>
		/// <remarks>
		/// This is an artifact of how joins are parsed.
		/// </remarks>
		public void AddPreviousJoin(JoinType type, Expression on_expression) {
			join_set.AddPreviousJoin(type, on_expression);
		}

		///<summary>
		/// Adds a Join to the from clause.
		///</summary>
		///<param name="type">The type of the join.</param>
		///<param name="on_expression">The expression representing the <c>ON</c>
		/// condition.</param>
		public void addJoin(JoinType type, Expression on_expression) {
			join_set.AddJoin(type, on_expression);
		}

		/// <summary>
		/// Returns the JoiningSet object for the FROM clause.
		/// </summary>
		public JoiningSet JoinSet {
			get { return join_set; }
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
		/// Returns a <see cref="ICollection">collection</see> of <see cref="FromTableDef"/> 
		/// objects that represent all the tables that are in this from clause.
		///</summary>
		public ICollection AllTables {
			get { return def_list; }
		}

		/// <inheritdoc/>
		public void PrepareExpressions(IExpressionPreparer preparer) {
			// Prepare expressions in the JoiningSet first
			int size = join_set.TableCount - 1;
			for (int i = 0; i < size; ++i) {
				Expression exp = join_set.GetOnExpression(i);
				if (exp != null) {
					exp.Prepare(preparer);
				}
			}
			// Prepare the StatementTree sub-queries in the from tables
			for (int i = 0; i < def_list.Count; ++i) {
				FromTableDef table_def = (FromTableDef)def_list[i];
				table_def.PrepareExpressions(preparer);
			}

		}

		/// <inheritdoc/>
		public Object Clone() {
			FromClause v = (FromClause)MemberwiseClone();
			v.join_set = (JoiningSet)join_set.Clone();
			ArrayList cloned_def_list = new ArrayList(def_list.Count);
			v.def_list = cloned_def_list;
			v.all_table_names = (ArrayList)all_table_names.Clone();

			for (int i = 0; i < def_list.Count; ++i) {
				FromTableDef table_def = (FromTableDef)def_list[i];
				cloned_def_list.Add(table_def.Clone());
			}

			return v;
		}
	}
}