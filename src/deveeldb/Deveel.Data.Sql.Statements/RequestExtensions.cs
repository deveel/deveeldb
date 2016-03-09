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
using System.Linq;

using Deveel.Data.Sql.Compile;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public static class RequestExtensions {
		public static ITable ExecuteStatement(this IRequest request, SqlStatement statement) {
			var results = request.ExecuteStatements(statement);
			return results[0];
		}

		public static ITable[] ExecuteStatements(this IRequest request, params SqlStatement[] statements) {
			return ExecuteStatements(request, null, statements);
		}

		public static ITable[] ExecuteStatements(this IRequest request, IExpressionPreparer preparer, params SqlStatement[] statements) {
			if (statements == null)
				throw new ArgumentNullException("statements");
			if (statements.Length == 0)
				throw new ArgumentException("No statements provided for execution", "statements");

			var results = new ITable[statements.Length];
			for (int i = 0; i < statements.Length; i++) {
				var statement = statements[i];

				var context = new ExecutionContext(request);

				var prepared = statement.Prepare(request, preparer);

				if (prepared == null)
					throw new InvalidOperationException(String.Format("The preparation of the statement '{0}' returned a null instance", statement.GetType()));

				prepared.Execute(context);

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

			return request.ExecuteStatements(preparer, compileResult.Statements.ToArray());
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
				var obj = new Field(parameter.SqlType, value);

				return SqlExpression.Constant(obj);
			}
		}

		#endregion

	}
}