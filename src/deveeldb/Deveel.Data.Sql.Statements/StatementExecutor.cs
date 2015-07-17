using System;
using System.Collections.Generic;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

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

			var results = new List<ITable>();
			foreach (var statement in statements) {
				// TODO: Invoke diagnostics for the preparation...

				var prepared = statement.Prepare(preparer, context);

				ITable result;

				try {
					result = prepared.Evaluate(context);
				} catch (Exception) {
					// TODO: Invoke diagnostics here before throwing the exception
					throw;
				}

				results.Add(result);
			}

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
