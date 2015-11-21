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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public static class QueryContextExtensions {
		public static ITable[] ExecuteQuery(this IQueryContext context, SqlQuery query) {
			return StatementExecutor.Execute(context, query);
		}

		public static ITable[] ExecuteQuery(this IQueryContext context, string sqlSource, params QueryParameter[] parameters) {
			var query = new SqlQuery(sqlSource);
			if (parameters != null) {
				foreach (var parameter in parameters) {
					query.Parameters.Add(parameter);
				}
			}

			return context.ExecuteQuery(query);
		}

		public static ITable[] ExecuteQuery(this IQueryContext context, string sqlSource) {
			return context.ExecuteQuery(sqlSource, null);
		}

		// TODO: Provide an overload with dynamic object parameter

		#region CreateView

		public static ITable ExecuteCreateView(this IQueryContext context, string viewName, string querySource) {
			return ExecuteCreateView(context, viewName, new string[0], querySource);
		}

		public static ITable ExecuteCreateView(this IQueryContext context, string viewName, IEnumerable<string> columnNames, string querySource) {
			var expression = SqlExpression.Parse(querySource);
			if (expression.ExpressionType != SqlExpressionType.Query)
				throw new ArgumentException("The input query string is invalid.", "querySource");

			return context.ExecuteCreateView(viewName, columnNames, (SqlQueryExpression) expression);
		}

		public static ITable ExecuteCreateView(this IQueryContext context, string viewName, SqlQueryExpression queryExpression) {
			return ExecuteCreateView(context, viewName, new string[0], queryExpression);
		}

		public static ITable ExecuteCreateView(this IQueryContext context, string viewName, IEnumerable<string> columnNames,
			SqlQueryExpression queryExpression) {
			var statement = new CreateViewStatement(viewName, columnNames, queryExpression);
			return statement.Execute(context);
		}

		#endregion

		#region Code Block Calls

		public static void GoTo(this IQueryContext context, string label) {
			var blockContext = context as IBlockQueryContext;
			if (blockContext == null)
				throw new InvalidOperationException("Not in a code block context");

			blockContext.GoTo(label);
		}

		public static void ControlLoop(this IQueryContext context, LoopControlType controlType) {
			ControlLoop(context, controlType, null);
		}

		public static void ControlLoop(this IQueryContext context, LoopControlType controlType, string label) {
			var blockContext = context as IBlockQueryContext;
			if (blockContext == null)
				throw new InvalidOperationException("Not in a code block context");

			blockContext.ControlLoop(controlType, label);
		}

		public static void Exit(this IQueryContext context) {
			Exit(context, null);
		}

		public static void Exit(this IQueryContext context, string label) {
			context.ControlLoop(LoopControlType.Exit, label);
		}

		public static void Continue(this IQueryContext context) {
			Continue(context, null);
		}

		public static void Continue(this IQueryContext context, string label) {
			context.ControlLoop(LoopControlType.Continue, label);
		}

		public static void Break(this IQueryContext context) {
			Break(context, null);
		}

		public static void Break(this IQueryContext context, string label) {
			context.ControlLoop(LoopControlType.Break, label);
		}

		public static void Raise(this IQueryContext context, string exceptionName) {
			var blockContext = context as IBlockQueryContext;
			if (blockContext == null)
				throw new InvalidOperationException("Not in a code block context");

			blockContext.Raise(exceptionName);
		}

		#endregion
	}
}
