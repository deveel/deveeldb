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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data {
	public static class QueryExtensions {
		public static bool IgnoreIdentifiersCase(this IQuery query) {
			return query.Context.IgnoreIdentifiersCase();
		}

		public static void IgnoreIdentifiersCase(this IQuery query, bool value) {
			query.Context.IgnoreIdentifiersCase(value);
		}

		public static void AutoCommit(this IQuery query, bool value) {
			query.Context.AutoCommit(value);
		}

		public static bool AutoCommit(this IQuery query) {
			return query.Context.AutoCommit();
		}

		public static string CurrentSchema(this IQuery query) {
			return query.Context.CurrentSchema();
		}

		public static void CurrentSchema(this IQuery query, string value) {
			query.Context.CurrentSchema(value);
		}

		public static void ParameterStyle(this IQuery query, QueryParameterStyle value) {
			query.Context.ParameterStyle(value);
		}

		public static QueryParameterStyle ParameterStyle(this IQuery query) {
			return query.Context.ParameterStyle();
		}

		#region Statements

		#region CreateTable

		public static void CreateTable(this IQuery query, ObjectName tableName, params SqlTableColumn[] columns) {
			CreateTable(query, tableName, false, columns);
		}

		public static void CreateTable(this IQuery query, ObjectName tableName, bool ifNotExists, params SqlTableColumn[] columns) {
			var statement = new CreateTableStatement(tableName, columns);
			statement.IfNotExists = ifNotExists;
			query.ExecuteStatement(statement);
		}

		public static void CreateTemporaryTable(this IQuery query, ObjectName tableName, params SqlTableColumn[] columns) {
			var statement = new CreateTableStatement(tableName, columns);
			statement.Temporary = true;
			query.ExecuteStatement(statement);
		}

		#endregion

		#region DropTable

		public static void DropTable(this IQuery query, ObjectName tableName) {
			DropTable(query, tableName, false);
		}

		public static void DropTable(this IQuery query, ObjectName tableName, bool ifExists) {
			query.ExecuteStatement(new DropTableStatement(tableName, ifExists));
		}

		#endregion

		#region CreateView

		public static void CreateView(this IQuery query, string viewName, string querySource) {
			CreateView(query, viewName, new string[0], querySource);
		}

		public static void CreateView(this IQuery query, string viewName, IEnumerable<string> columnNames, string querySource) {
			var expression = SqlExpression.Parse(querySource);
			if (expression.ExpressionType != SqlExpressionType.Query)
				throw new ArgumentException("The input query string is invalid.", "querySource");

			query.CreateView(viewName, columnNames, (SqlQueryExpression)expression);
		}

		public static void CreateView(this IQuery query, string viewName, SqlQueryExpression queryExpression) {
			CreateView(query, viewName, new string[0], queryExpression);
		}

		public static void CreateView(this IQuery query, string viewName, IEnumerable<string> columnNames,
			SqlQueryExpression queryExpression) {
			var statement = new CreateViewStatement(viewName, columnNames, queryExpression);
			query.ExecuteStatement(statement);
		}

		#endregion

		#region DropView

		public static void DropView(this IQuery query, ObjectName viewName) {
			DropView(query, viewName, false);
		}

		public static void DropView(this IQuery query, ObjectName viewName, bool ifExists) {
			query.ExecuteStatement(new DropViewStatement(viewName, ifExists));
		}

		#endregion

		#endregion
	}
}
