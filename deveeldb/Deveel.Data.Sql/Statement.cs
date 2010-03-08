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
using System.Collections;

using Deveel.Data.Client;
using Deveel.Diagnostics;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Provides a set of useful utility functions to use by all the
	/// interpretted statements.
	/// </summary>
	public abstract class Statement {
		internal Statement() {
			info = new StatementTree(GetType());
		}

		/// <summary>
		/// The Database context.
		/// </summary>
		private DatabaseConnection database;

		/// <summary>
		/// The user context.
		/// </summary>
		private User user;

		/// <summary>
		/// The StatementTree object that is the container for the query.
		/// </summary>
		private StatementTree info;

		/// <summary>
		/// The SqlQuery object that was used to produce this statement.
		/// </summary>
		private SqlQuery query;

		/// <summary>
		/// The list of all IFromTableSource objects of resources referenced 
		/// in this query.
		/// </summary>
		private ArrayList table_list = new ArrayList();

		/// <summary>
		/// Gets a connection to the database where the statement will be executed.
		/// </summary>
		protected DatabaseConnection Connection {
			get { return database; }
		}

		/// <summary>
		/// Gets the user that is executing the statement.
		/// </summary>
		protected User User {
			get { return user; }
		}

		/// <summary>
		/// Gets the <see cref="SqlQuery">Query</see> object that was used to 
		/// produce the statement.
		/// </summary>
		protected SqlQuery Query {
			get { return query; }
		}

		internal StatementTree Info {
			get { return info; }
		}

		/// <summary>
		/// Gets an <see cref="IDebugLogger"/> used to log _queries.
		/// </summary>
		protected IDebugLogger Debug {
			get { return database.Debug; }
		}

		protected string GetString(string key) {
			return (string)info.GetObject(key);
		}

		protected object GetValue(string key) {
			return info.GetObject(key);
		}

		protected int GetInteger(string key) {
			return info.GetInt(key);
		}

		protected bool GetBoolean(string key) {
			return info.GetBoolean(key);
		}

		protected Expression GetExpression(string key) {
			return (Expression) GetValue(key);
		}

		protected IList GetList(string key) {
			return (IList) GetValue(key);
		}

		protected void SetValue(string key, object value) {
			info.SetObject(key, value);
		}

		protected void SetValue(string key, int value) {
			SetValue(key, (object)value);
		}

		protected void SetValue(string key, bool value) {
			SetValue(key, (object)value);
		}

		protected void SetValue(string key, string value) {
			SetValue(key, (object)value);
		}

		protected void SetValue(string key, Expression value) {
			SetValue(key, (object)value);
		}

		/// <summary>
		/// Resets this statement so it may be re-prepared and evaluated again.
		/// </summary>
		/// <remarks>
		/// Useful for repeating a query multiple times.
		/// </remarks>
		internal virtual void Reset() {
			database = null;
			user = null;
			table_list = new ArrayList();
		}

		/// <summary>
		/// Performs initial preparation on the contents of the 
		/// <see cref="StatementTree"/> by resolving all sub queries and 
		/// mapping functions to their executable forms.
		/// </summary>
		/// <remarks>
		/// Given a <see cref="StatementTree"/> and a <see cref="Database"/> 
		/// context, this method will convert all sub-queries found in 
		/// <see cref="StatementTree"/> to a <i>queriable</i> object. 
		/// In other words, all <see cref="StatementTree"/> are converted to 
		/// <see cref="SelectStatement"/>.
		/// <para>
		/// This is called after <see cref="Init"/> and before <see cref="Prepare"/>.
		/// </para>
		/// </remarks>
		internal void ResolveTree() {
			// For every expression in this select we must go through and resolve
			// any sub-queries we find to the correct Select object.
			// This method will prepare the sub-query substitute the StatementTree
			// object for a Select object in the expression.
			IExpressionPreparer preparer = new ExpressionPreparerImpl(database);
			info.PrepareAllExpressions(preparer);
		}

		private class ExpressionPreparerImpl : IExpressionPreparer {
			private readonly DatabaseConnection database;

			public ExpressionPreparerImpl(DatabaseConnection database) {
				this.database = database;
			}

			public bool CanPrepare(Object element) {
				return element is StatementTree;
			}

			public object Prepare(Object element) {
				StatementTree stmt_tree = (StatementTree)element;
				SelectStatement stmt = new SelectStatement();
				stmt.Init(database, stmt_tree, null);
				stmt.ResolveTree();
				stmt.Prepare();
				return stmt;
			}
		}

		/// <summary>
		/// Attempts to find the table that the given column is in.
		/// </summary>
		/// <param name="column_name">The name of the column to find the
		/// table for.</param>
		/// <returns>
		/// Returns a <see cref="IFromTableSource"/> representing the table the 
		/// column with the given name is in, if found, otherwise <b>null</b>.
		/// </returns>
		internal IFromTableSource FindTableWithColumn(VariableName column_name) {
			for (int i = 0; i < table_list.Count; ++i) {
				IFromTableSource table = (IFromTableSource)table_list[i];
				TableName tname = column_name.TableName;
				String sch_name = null;
				String tab_name = null;
				String col_name = column_name.Name;
				if (tname != null) {
					sch_name = tname.Schema;
					tab_name = tname.Name;
				}
				int rcc = table.ResolveColumnCount(null, sch_name, tab_name, col_name);
				if (rcc > 0) {
					return table;
				}
			}
			return null;
		}

		/// <summary>
		/// Checks the existence of a table defining a column with the
		/// given name.
		/// </summary>
		/// <param name="column_name"></param>
		/// <returns>
		/// Returns <b>true</b> if a table containing a column with the 
		/// given name exists, otherwise <b>false</b>.
		/// </returns>
		internal bool ExistsTableWithColumn(VariableName column_name) {
			return FindTableWithColumn(column_name) != null;
		}

		/// <summary>
		/// Resolves the given alias name against the columns defined
		///  by the statement (if it has aliasing capabilities).
		/// </summary>
		/// <param name="alias_name"></param>
		/// <returns>
		/// Returns a list of all fully qualified <see cref="VariableName"/> 
		/// that match the alias name, or an empty array if no matches 
		/// found.
		/// </returns>
		internal virtual ArrayList ResolveAgainstAliases(VariableName alias_name) {
			return new ArrayList(0);
		}

		/// <summary>
		/// Resolves table name over the given session.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="db"></param>
		/// <remarks>
		/// If the schema part of the table name is not present then it 
		/// is set to the current schema of the database session. If the
		/// database is ignoring the case then this will correctly resolve 
		/// the table to the cased version of the table name.
		/// </remarks>
		/// <returns></returns>
		internal TableName ResolveTableName(string name, DatabaseConnection db) {
			return db.ResolveTableName(name);
		}

		internal TableName ResolveTableName(string name) {
			return database.ResolveTableName(name);
		}

		/// <summary>
		/// Finds a table present in the query for the given schema and
		/// table name.
		/// </summary>
		/// <param name="schema"></param>
		/// <param name="name"></param>
		/// <returns>
		/// Returns the first <see cref="IFromTableSource"/> that matches the given 
		/// schema, table reference, or <b>null</b> if no objects with the 
		/// given schema/name reference match.
		/// </returns>
		internal IFromTableSource FindTableInQuery(String schema, String name) {
			for (int p = 0; p < table_list.Count; ++p) {
				IFromTableSource table = (IFromTableSource)table_list[p];
				if (table.MatchesReference(null, schema, name)) {
					return table;
				}
			}
			return null;
		}

		/// <summary>
		/// Attempts to resolve an ambiguous column name (such as <i>id</i>)
		/// into a <see cref="VariableName"/> from the tables in this statement.
		/// </summary>
		/// <param name="v">The column name to resolve.</param>
		/// <returns></returns>
		internal VariableName ResolveColumn(VariableName v) {
			// Try and resolve against alias names first,
			ArrayList list = new ArrayList();
			list.AddRange(ResolveAgainstAliases(v));

			TableName tname = v.TableName;
			String sch_name = null;
			String tab_name = null;
			String col_name = v.Name;
			if (tname != null) {
				sch_name = tname.Schema;
				tab_name = tname.Name;
			}

			int matches_found = 0;
			// Find matches in our list of tables sources,
			for (int i = 0; i < table_list.Count; ++i) {
				IFromTableSource table = (IFromTableSource)table_list[i];
				int rcc = table.ResolveColumnCount(null, sch_name, tab_name, col_name);
				if (rcc == 1) {
					VariableName matched = table.ResolveColumn(null, sch_name, tab_name, col_name);
					list.Add(matched);
				} else if (rcc > 1) {
					throw new StatementException("Ambiguous column name (" + v + ")");
				}
			}

			int total_matches = list.Count;
			if (total_matches == 0) {
				throw new StatementException("Can't find column: " + v);
			} else if (total_matches == 1) {
				return (VariableName)list[0];
			} else if (total_matches > 1) {
				// if there more than one match, check if they all match the identical
				// resource,
				throw new StatementException("Ambiguous column name (" + v + ")");
			} else {
				// Should never reach here but we include this exception to keep the
				// compiler happy.
				throw new ApplicationException("Negative total matches?");
			}

		}

		///<summary>
		/// Given a Variable object, this will resolve the name into a column name
		/// the database understands (substitutes aliases, etc).
		///</summary>
		///<param name="v"></param>
		///<returns></returns>
		public VariableName ResolveVariableName(VariableName v) {
			return ResolveColumn(v);
		}

		/// <summary>
		/// Given an <see cref="Expression"/>, this will run through the expression 
		/// and resolve any variable names via the <see cref="ResolveVariableName"/> 
		/// method here.
		/// </summary>
		/// <param name="exp"></param>
		internal void ResolveExpression(Expression exp) {
			// NOTE: This gets variables in all function parameters.
			IList vars = exp.AllVariables;
			for (int i = 0; i < vars.Count; ++i) {
				VariableName v = (VariableName)vars[i];
				VariableName to_set = ResolveVariableName(v);
				v.Set(to_set);
			}
		}

		/// <summary>
		/// Add an <see cref="IFromTableSource"/> used within the query.
		/// </summary>
		/// <param name="table">The table to add to the query.</param>
		/// <remarks>
		/// These tables are used when we try to resolve a column name.
		/// </remarks>
		protected void AddTable(IFromTableSource table) {
			table_list.Add(table);
		}

		/// <summary>
		/// Sets up internal variables for this statement for derived 
		/// classes to use.
		/// </summary>
		/// <param name="db">The session that will execute the statement.</param>
		/// <param name="stree">The <see cref="StatementTree"/> that contains 
		/// the parsed content of the statement being executed.</param>
		/// <param name="query"></param>
		/// <remarks>
		/// This is called before <see cref="Prepare"/> and <see cref="LockingMechanism.IsInExclusiveMode"/>
		/// is called.
		/// <para>
		/// It is assumed that any <i>?</i> style parameters in the 
		/// <paramref name="stree"/> will have been resolved previous to 
		/// a call to this method.
		/// </para>
		/// </remarks>
		internal void Init(DatabaseConnection db, StatementTree stree, SqlQuery query) {
			database = db;
			user = db.User;
			info = stree;
			this.query = query;
		}

		/// <summary>
		/// Prepares the statement with the given database object.
		/// </summary>
		/// <remarks>
		/// This is called before the statement is evaluated. The prepare 
		/// statement queries the database and resolves information about 
		/// the statement (for example, it resolves column names and aliases 
		/// and determines the tables that are touched by this statement 
		/// so we can lock the appropriate tables before we evaluate).
		/// <para>
		/// <b>Note:</b> Care must be taken to ensure that all methods 
		/// called here are safe in as far as modifications to the data 
		/// occuring. 
		/// The rules for safety should be as follows: 
		/// <list type="bullet">
		/// <item>
		/// If the database is in <see cref="LockingMode.Exclusive"/> mode, 
		/// then we need to wait until it's switched back to 
		/// <see cref="LockingMode.Shared"/> mode before this method is called.
		/// </item>
		/// <item>
		/// All collection of information done here should not involve 
		/// any table state info. Except for column count, column names, 
		/// column types, etc.
		/// </item>
		/// <item>
		/// Queries such as obtaining the row count, selectable scheme 
		/// information, and certainly 'GetCellValue' must never be called 
		/// during prepare.
		/// </item>
		/// <item>
		/// When prepare finishes, the affected tables are locked and 
		/// the query is safe to call <see cref="Evaluate"/> at which 
		/// time table state is safe to inspect.
		/// </item>
		/// </list>
		/// </para>
		/// </remarks>
		/// <exception cref="DatabaseException"/>
		internal abstract void Prepare();

		/// <summary>
		/// Evaluates the statement after it is prepared.
		/// </summary>
		/// <remarks>
		/// The method must be called after <see cref="Prepare"/>.
		/// </remarks>
		/// <returns>
		/// Returns a table that represents the result set.
		/// </returns>
		/// <exception cref="DatabaseException"/>
		/// <exception cref="TransactionException"/>
		internal abstract Table Evaluate();
	}
}