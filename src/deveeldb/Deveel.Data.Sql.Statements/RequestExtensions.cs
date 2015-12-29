using System;
using System.Collections.Generic;

using Deveel.Data.Diagnostics;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Parser;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public static class RequestExtensions {
		public static ITable ExecuteStatement(this IRequest request, IStatement statement) {
			var results = request.ExecuteStatements(statement);
			return results[0];
		}

		public static ITable[] ExecuteStatements(this IRequest request, params IStatement[] statements) {
			if (statements == null)
				throw new ArgumentNullException("statements");
			if (statements.Length == 0)
				throw new ArgumentException();

			var results = new ITable[statements.Length];
			for (int i = 0; i < statements.Length; i++) {
				var statement = statements[i];

				var context = new ExecutionContext(request);

				if (statement is IPreparableStatement)
					statement = ((IPreparableStatement) statement).Prepare(request);

				statement.Execute(context);

				ITable result;
				if (context.HasResult) {
					result = context.Result;
				} else {
					result = FunctionTable.ResultTable(request, 0);
				}

				results[i] = result;
			}

			return results;
		}

		public static ITable[] Execute(this IRequest request, SqlQuery query) {
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

				// TODO: query.RegisterQuery(statement);

				// TODO: Invoke diagnostics for the preparation...

				var prepared = statement.Prepare(preparer, request);

				ITable result;

				try {
					var exeContext = new ExecutionContext(request);
					prepared.Execute(exeContext);
					if (exeContext.HasResult) {
						result = exeContext.Result;
					} else {
						result = FunctionTable.ResultTable(request, 0);
					}
				} catch (StatementException ex) {
					request.OnError(ex);
					throw;
				} catch (Exception ex) {
					var sex = new StatementException("An unhanded error occurred while executing the statement.", ex);
					request.OnError(sex);
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

				var value = parameter.SqlType.CreateFrom(parameter.Value);
				var obj = new DataObject(parameter.SqlType, value);

				return SqlExpression.Constant(obj);
			}
		}

		#endregion

	}
}