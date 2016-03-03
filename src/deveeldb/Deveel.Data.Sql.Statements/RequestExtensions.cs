using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Diagnostics;
using Deveel.Data.Sql.Compile;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Parser;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public static class RequestExtensions {
		public static ITable ExecuteStatement(this IRequest request, ISqlCodeObject statement) {
			var results = request.ExecuteStatements(statement);
			return results[0];
		}

		public static ITable[] ExecuteStatements(this IRequest request, params ISqlCodeObject[] statements) {
			return ExecuteStatements(request, null, statements);
		}

		public static ITable[] ExecuteStatements(this IRequest request, IExpressionPreparer preparer, params ISqlCodeObject[] statements) {
			if (statements == null)
				throw new ArgumentNullException("statements");
			if (statements.Length == 0)
				throw new ArgumentException();

			var results = new ITable[statements.Length];
			for (int i = 0; i < statements.Length; i++) {
				var statement = statements[i];

				var context = new ExecutionContext(request);

				if (statement is IPreparableStatement)
					statement = ((IPreparableStatement) statement).Prepare(preparer, request);

				var executable = statement as IExecutable;

				if (executable == null)
					throw new InvalidOperationException();

				executable.Execute(context);

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
			var compiler = request.Context.SqlCompiler();

			var compileResult = compiler.Compile(new SqlCompileContext(request.Context, sqlSouce));

			if (compileResult.HasErrors) {
				// TODO: throw a specialized exception...
				throw new InvalidOperationException();
			}

			var preparer = new QueryPreparer(query);

			return request.ExecuteStatements(preparer, compileResult.CodeObjects.ToArray());
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