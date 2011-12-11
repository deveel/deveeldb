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
using System.Data;
using System.IO;

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
	public static class SqlQueryExecutor {
		/// <summary>
		/// The SQL parser state.
		/// </summary>
		private readonly static SQL SqlParser;

		static SqlQueryExecutor() {
			// Set up the sql parser.
			SqlParser = new SQL(new StringReader(""));
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
		public static Table Execute(DatabaseConnection connection, SqlQuery query) {
			// StatementTree caching

			// Create a new parser and set the parameters...
			string commandText = query.Text;
			StatementTree statementTree = null;
			StatementCache statementCache = connection.System.StatementCache;

			if (statementCache != null)
				// Is this Query cached?
				statementTree = statementCache.Get(commandText);

			if (statementTree == null) {
				try {
					lock (SqlParser) {
						SqlParser.ReInit(new StringReader(commandText));
						SqlParser.Reset();
						// Parse the statement.
						statementTree = SqlParser.Statement();
					}
				} catch (ParseException e) {
					throw new SqlParseException(e, commandText);
				}

				// Put the statement tree in the cache
				if (statementCache != null)
					statementCache.Set(commandText, statementTree);
			}

			// Substitute all parameter substitutions in the statement tree.
			IExpressionPreparer preparer = new QueryPreparer(query);
			statementTree.PrepareAllExpressions(preparer);

			// Convert the StatementTree to a statement object
			Statement statement;
			Type statementType = statementTree.StatementType;
			try {
				statement = (Statement)Activator.CreateInstance(statementType);
			} catch (TypeLoadException) {
				throw new DataException("Could not find statement type: " + statementType);
			} catch (TypeInitializationException) {
				throw new DataException("Could not instantiate type: " + statementType);
			} catch (AccessViolationException) {
				throw new DataException("Could not access type: " + statementType);
			}


			// Initialize the statement
			statement.Context.Set(connection, statementTree, query);

			// Evaluate the SQL statement.
			return statement.EvaluateStatement();
		}

		private class QueryPreparer : IExpressionPreparer {
			private readonly SqlQuery query;

			public QueryPreparer(SqlQuery query) {
				this.query = query;
			}

			public bool CanPrepare(Object element) {
				return (element is ParameterSubstitution);
			}

			public object Prepare(object element) {
				ParameterSubstitution ps = (ParameterSubstitution)element;
				object value;
				if (query.ParameterStyle == ParameterStyle.Named) {
					string paramName = ps.Name;
					value = query.GetNamedVariable(paramName);
				} else {
					int paramId = ps.Id;
					value = query.Variables[paramId];
				}
				return TObject.CreateObject(value);
			}
		}
	}
}