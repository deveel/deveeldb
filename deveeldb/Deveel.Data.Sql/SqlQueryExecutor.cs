//  
//  SqlCommand.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Data;
using System.IO;

using Deveel.Data.Client;

namespace Deveel.Data.Sql {
	///<summary>
	/// An object used to execute SQL queries against a given 
	/// <see cref="DatabaseConnection"/> object.
	///</summary>
	/// <remarks>
	/// The object maintains an SQL parser object as state which is reused 
	/// as necessary.
	/// <para>
	/// This object is a convenient way to execute SQL _queries.
	/// </para>
	/// </remarks>
	public class SqlQueryExecutor {
		/// <summary>
		/// The SQL parser state.
		/// </summary>
		private readonly static SQL sql_parser;

		static SqlQueryExecutor() {
			// Set up the sql parser.
			sql_parser = new SQL(new StringReader(""));
		}

		///<summary>
		/// Executes the given <see cref="SqlQuery"/> object on the given 
		/// <see cref="DatabaseConnection"/> object.
		///</summary>
		///<param name="connection"></param>
		///<param name="query"></param>
		/// <remarks>
		/// This method does not perform any locking. Any locking must have happened 
		/// before this method is called.
		/// <para>
		/// Also the returned <see cref="Table"/> object is onld valid within the
		/// life-time of the lock unless the root lock requirements are satisified.
		/// </para>
		/// </remarks>
		///<returns>
		/// Returns a <see cref="Table"/> object that contains the result of the execution.
		/// </returns>
		///<exception cref="DataException"></exception>
		public Table Execute(DatabaseConnection connection, SqlQuery query) {

			// StatementTree caching

			// Create a new parser and set the parameters...
			String commandText = query.Text;
			StatementTree statement_tree = null;
			StatementCache statement_cache = connection.System.StatementCache;

			if (statement_cache != null) {
				// Is this Query cached?
				statement_tree = statement_cache.Get(commandText);
			}
			if (statement_tree == null) {
				try {
					lock (sql_parser) {
						sql_parser.ReInit(new StringReader(commandText));
						sql_parser.Reset();
						// Parse the statement.
						statement_tree = sql_parser.Statement();
					}
				} catch (ParseException e) {
					throw new SqlParseException(e, commandText);
				}

				// Put the statement tree in the cache
				if (statement_cache != null) {
					statement_cache.Set(commandText, statement_tree);
				}
			}

			// Substitute all parameter substitutions in the statement tree.
			IExpressionPreparer preparer = new ExpressionPreparerImpl(query);
			statement_tree.PrepareAllExpressions(preparer);

			// Convert the StatementTree to a statement object
			Statement statement;
			Type statement_class = statement_tree.StatementType;
			try {
				statement = (Statement)Activator.CreateInstance(statement_class);
			} catch (TypeLoadException) {
				throw new DataException("Could not find statement class: " + statement_class);
			} catch (TypeInitializationException e) {
				throw new DataException("Could not instantiate class: " + statement_class);
			} catch (AccessViolationException e) {
				throw new DataException("Could not access class: " + statement_class);
			}


			// Initialize the statement
			statement.Init(connection, statement_tree, query);

			// Automated statement tree preparation
			statement.ResolveTree();

			// Prepare the statement.
			statement.Prepare();

			// Evaluate the SQL statement.
			Table result = statement.Evaluate();

			return result;

		}

		private class ExpressionPreparerImpl : IExpressionPreparer {
			private SqlQuery query;

			public ExpressionPreparerImpl(SqlQuery query) {
				this.query = query;
			}

			public bool CanPrepare(Object element) {
				return (element is ParameterSubstitution);
			}

			public Object Prepare(Object element) {
				ParameterSubstitution ps = (ParameterSubstitution)element;
				object value;
				if (query.ParameterStyle == ParameterStyle.Named) {
					string param_name = ps.Name;
					value = query.GetNamedVariable(param_name);
				} else {
					int param_id = ps.Id;
					value = query.Variables[param_id];
				}
				return TObject.GetObject(value);
			}
		}
	}
}