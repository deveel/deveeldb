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
using System.Collections.Generic;

using Deveel.Data.DbSystem;
using Deveel.Data.Diagnostics;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Parser;

namespace Deveel.Data.Sql.Statements {
	/// <summary>
	/// This class is used to transform an input query to a set of statements
	/// and execute them within a given context.
	/// </summary>
	public static class StatementExecutor {
		/// <summary>
		/// This method transforms the input SQL query into a set of statements,
		/// prepares and executes them against the provided context.
		/// </summary>
		/// <param name="context">The context used to prepare and execute the statements
		/// resolved from the compilation of the input query.</param>
		/// <param name="query">The input SQL query with optional parameters, that is
		/// compiled into a set of statements to be executed.</param>
		/// <remarks>
		/// The method first tries to resolve the compiled statements from the specialized
		/// cache (<see cref="StatementCache"/>) from the system, to reduce the compilation time.
		/// </remarks>
		/// <returns>
		/// Returns an array of <see cref="ITable"/> objects that represent the results
		/// of the execution of the input query.
		/// </returns>
		public static ITable[] Execute(IQueryContext context, SqlQuery query) {
			if (query == null)
				throw new ArgumentNullException("query");

			var sqlSouce = query.Text;

			// TODO: find it from the cache...

			var statements = SqlStatement.Parse(sqlSouce);

			// TODO: set it in cache ...

			var preparer = new QueryPreparer(query);

			bool statementSeen = false;

			var results = new List<ITable>();
			foreach (var statement in statements) {
				context.RegisterQuery(statement);

				// TODO: Invoke diagnostics for the preparation...

				var prepared = statement.Prepare(preparer, context);

				ITable result;

				try {
					result = prepared.Evaluate(context);
				} catch(StatementException ex) {
					context.RegisterError(ex);
					throw;
				} catch (Exception ex) {
					var sex = new StatementException("An unhanded error occurred while executing the statement.", ex);
					context.RegisterError(sex);
					throw sex;
				} finally {
					statementSeen = true;
				}

				results.Add(result);
			}

			if (!statementSeen)
				throw new SqlParseException("The input query was not parsed in any statements that could be executed.");

			return results.ToArray();
		}

		#region QueryPreparer

		class QueryPreparer : IExpressionPreparer {
			private readonly SqlQuery query;

			public QueryPreparer(SqlQuery query) {
				this.query = query;
			}

			public bool CanPrepare(SqlExpression expression) {
				return expression is SqlVariableReferenceExpression;
			}

			public SqlExpression Prepare(SqlExpression expression) {
				var varRef = (SqlVariableReferenceExpression) expression;
				var varName = varRef.VariableName;

				var parameter = query.Parameters.FindParameter(varName);
				if (parameter == null)
					return expression;

				var value = parameter.DataType.CreateFrom(parameter.Value);
				var obj = new DataObject(parameter.DataType, value);

				return SqlExpression.Constant(obj);
			}
		}

		#endregion
	}
}
