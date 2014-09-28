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
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;

using Deveel.Data.DbSystem;
using Deveel.Data.Text;
using Deveel.Data.Types;

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
		public static Table[] Execute(IDatabaseConnection connection, SqlQuery query) {
			// StatementTree caching

			// Create a new parser and set the parameters...
			string commandText = query.Text;
			IList<StatementTree> statementTreeList = null;
			StatementCache statementCache = connection.Database.Context.StatementCache;

			if (statementCache != null)
				// Is this Query cached?
				statementTreeList = statementCache.Get(commandText);

			if (statementTreeList == null) {
				try {
					lock (SqlParser) {
						SqlParser.ReInit(new StreamReader(new MemoryStream(Encoding.Unicode.GetBytes(commandText)), Encoding.Unicode));
						SqlParser.Reset();
						// Parse the statement.
						statementTreeList = SqlParser.StatementList();
					}
				} catch (ParseException e) {
					var tokens = SqlParser.token_source.tokenHistory;
					throw new SqlParseException(e, commandText, tokens);
				}

				// Put the statement tree in the cache
				if (statementCache != null)
					statementCache.Set(commandText, statementTreeList);
			}

			// Substitute all parameter substitutions in the statement tree.
			IExpressionPreparer preparer = new QueryPreparer(query);

			List<Table> results = new List<Table>(statementTreeList.Count);
			foreach (StatementTree statementTree in statementTreeList) {
				statementTree.PrepareAllExpressions(preparer);

				// Convert the StatementTree to a statement object
				Statement statement;
				Type statementType = statementTree.StatementType;
				try {
					statement = (Statement) Activator.CreateInstance(statementType);
				} catch (TypeLoadException) {
					throw new DataException("Could not find statement type: " + statementType);
				} catch (TypeInitializationException) {
					throw new DataException("Could not instantiate type: " + statementType);
				} catch (AccessViolationException) {
					throw new DataException("Could not access type: " + statementType);
				}

				statement.Query = query;
				statement.StatementTree = statementTree;

				DatabaseQueryContext context = new DatabaseQueryContext(connection);

				// Prepare the statement
				statement.PrepareStatement(context);

				// Evaluate the SQL statement.
				results.Add(statement.EvaluateStatement(context));
			}

			return results.ToArray();
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
				var ps = (ParameterSubstitution)element;
				SqlQueryParameter param = null;

				if (!String.IsNullOrEmpty(ps.Name)) {
					param = query.Parameters.GetNamedParameter(ps.Name);
				} else {
					param = query.Parameters.ElementAt(ps.Id);
				}

				if (param == null)
					throw new InvalidOperationException("Could not find the parameter in the query.");

				var type = CreateTType(param);
				var value = param.Value;

				return new TObject(type, value);
			}

			private static TType CreateTType(SqlQueryParameter parameter) {
				if (parameter.SqlType == SqlType.Bit)
					return PrimitiveTypes.Boolean;
				if (parameter.SqlType == SqlType.TinyInt ||
				    parameter.SqlType == SqlType.SmallInt ||
				    parameter.SqlType == SqlType.Integer ||
				    parameter.SqlType == SqlType.BigInt ||
				    parameter.SqlType == SqlType.Float ||
				    parameter.SqlType == SqlType.Real ||
				    parameter.SqlType == SqlType.Double ||
				    parameter.SqlType == SqlType.Numeric)
					// TODO: support the scale in parameter ...
					return TType.GetNumericType(parameter.SqlType, parameter.Size, 0);
				if (parameter.SqlType == SqlType.Char ||
				    parameter.SqlType == SqlType.VarChar)
					// TODO: Support the locale at least ...
					return TType.GetStringType(parameter.SqlType, parameter.Size, null, CollationStrength.None,
						CollationDecomposition.None);

				if (parameter.SqlType == SqlType.Binary ||
				    parameter.SqlType == SqlType.VarBinary)
					return TType.GetBinaryType(parameter.SqlType, parameter.Size);

				if (parameter.SqlType == SqlType.Date ||
				    parameter.SqlType == SqlType.Time ||
				    parameter.SqlType == SqlType.TimeStamp)
					return TType.GetDateType(parameter.SqlType);

				if (parameter.SqlType == SqlType.Null)
					return PrimitiveTypes.Null;

				if (parameter.SqlType == SqlType.LongVarBinary ||
					parameter.SqlType == SqlType.LongVarChar ||
					parameter.SqlType == SqlType.Blob ||
					parameter.SqlType == SqlType.Clob)
					throw new NotImplementedException("Support for LOBs in parameters not implemented yet.");

				throw new NotSupportedException(String.Format("Parameter of type {0} is not supported", parameter.SqlType));
			}
		}
	}
}