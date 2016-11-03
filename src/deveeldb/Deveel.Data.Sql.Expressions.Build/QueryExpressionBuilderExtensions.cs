// 
//  Copyright 2010-2016 Deveel
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

namespace Deveel.Data.Sql.Expressions.Build {
	public static class QueryExpressionBuilderExtensions {
		public static IQueryExpressionBuilder AllColumns(this IQueryExpressionBuilder builder) {
			return builder.Column(new ObjectName("*"));
		}

		public static IQueryExpressionBuilder Column(this IQueryExpressionBuilder builder, ObjectName columnName, string alias) {
			return builder.Item(item => item.Expression(SqlExpression.Reference(columnName)).As(alias));
		}

		public static IQueryExpressionBuilder Column(this IQueryExpressionBuilder builder, ObjectName columnName) {
			return builder.Column(columnName, null);
		}

		public static IQueryExpressionBuilder Column(this IQueryExpressionBuilder builder, string columnName, string alias) {
			return builder.Column(ObjectName.Parse(columnName), alias);
		}

		public static IQueryExpressionBuilder Column(this IQueryExpressionBuilder builder, string columnName) {
			return builder.Column(columnName, null);
		}

		public static IQueryExpressionBuilder Expression(this IQueryExpressionBuilder builder, SqlExpression expression,
			string alias) {
			return builder.Item(item => item.Expression(expression).As(alias));
		}

		public static IQueryExpressionBuilder Expression(this IQueryExpressionBuilder builder, SqlExpression expression) {
			return builder.Expression(expression, null);
		}

		public static IQueryExpressionBuilder Function(this IQueryExpressionBuilder builder, ObjectName functionName, SqlExpression[] args) {
			return builder.Function(functionName, args, null);
		}

		public static IQueryExpressionBuilder Function(this IQueryExpressionBuilder builder, ObjectName functionName,
			SqlExpression[] args, string alias) {
			return builder.Expression(SqlExpression.FunctionCall(functionName, args), alias);
		}

		public static IQueryExpressionBuilder Function(this IQueryExpressionBuilder builder, ObjectName functionName,
			string alias) {
			return builder.Function(functionName, new SqlExpression[0], alias);
		}

		public static IQueryExpressionBuilder Function(this IQueryExpressionBuilder builder, ObjectName functionName) {
			return builder.Function(functionName, (string) null);
		}

		public static IQueryExpressionBuilder Function(this IQueryExpressionBuilder builder, string functionName, SqlExpression[]args) {
			return builder.Function(functionName, args, null);
		}

		public static IQueryExpressionBuilder Function(this IQueryExpressionBuilder builder, string functionName) {
			return Function(builder, functionName, (string) null);
		}

		public static IQueryExpressionBuilder Function(this IQueryExpressionBuilder builder, string functionName, string alias) {
			return Function(builder, functionName, new SqlExpression[0], alias);
		}

		public static IQueryExpressionBuilder Function(this IQueryExpressionBuilder builder, string functionName, SqlExpression[] args, string alias) {
			return builder.Function(ObjectName.Parse(functionName), args, alias);
		}

		public static IQueryExpressionBuilder Constant(this IQueryExpressionBuilder builder, object value) {
			return Constant(builder, value, null);
		}

		public static IQueryExpressionBuilder Constant(this IQueryExpressionBuilder builder, object value, string alias) {
			return builder.Expression(SqlExpression.Constant(value), alias);
		}

		public static IQueryExpressionBuilder FromTable(this IQueryExpressionBuilder builder, ObjectName tableName) {
			return builder.FromTable(tableName, null);
		}

		public static IQueryExpressionBuilder FromTable(this IQueryExpressionBuilder builder, ObjectName tableName, string alias) {
			return builder.From(source => source.Table(tableName).As(alias));
		}

		public static IQueryExpressionBuilder FromTable(this IQueryExpressionBuilder builder, string tableName) {
			return builder.FromTable(tableName, null);
		}

		public static IQueryExpressionBuilder FromTable(this IQueryExpressionBuilder builder, string tableName, string alias) {
			return builder.FromTable(ObjectName.Parse(tableName), alias);
		}

		public static IQueryExpressionBuilder GroupBy(this IQueryExpressionBuilder builder, params Action<IExpressionBuilder>[] groupBy) {
			var expressions = new List<SqlExpression>();
			foreach (var action in groupBy) {
				var expBuilder = new ExpressionBuilder();
				action(expBuilder);

				expressions.Add(expBuilder.Build());
			}

			return builder.GroupBy(expressions.ToArray());
		}

		public static IQueryExpressionBuilder Where(this IQueryExpressionBuilder builder,
			Action<IExpressionBuilder> expression) {
			var expBuilder = new ExpressionBuilder();
			expression(expBuilder);

			return builder.Where(expBuilder.Build());
		}

		public static IQueryExpressionBuilder Having(this IQueryExpressionBuilder builder, Action<IExpressionBuilder> expression) {
			var expBuilder = new ExpressionBuilder();
			expression(expBuilder);

			return builder.Having(expBuilder.Build());
		}
	}
}
