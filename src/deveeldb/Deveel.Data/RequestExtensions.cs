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

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Compile;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

namespace Deveel.Data {
	public static class RequestExtensions {
		public static User User(this IRequest request) {
			return request.Query.Session.User;
		}

		public static string UserName(this IRequest request) {
			return request.User().Name;
		}

		private static int GetResult(ITable table) {
			if (table.RowCount != 1)
				throw new InvalidOperationException("Invalid number of rows returned in result");
			if (table.TableInfo.ColumnCount != 1)
				throw new InvalidOperationException("Invalid number of columns returned in result");

			var value = table.GetValue(0, 0);
			return value.AsInteger();
		}

		#region Statements

		#region Declare Cursor

		public static void DeclareCursor(this IRequest request, string cursorName, SqlQueryExpression query) {
			DeclareCursor(request, cursorName, CursorFlags.Insensitive, query);
		}

		public static void DeclareCursor(this IRequest request, string cursorName, CursorFlags flags, SqlQueryExpression query) {
			DeclareCursor(request, cursorName, new CursorParameter[0], flags, query);
		}

		public static void DeclareCursor(this IRequest request, string cursorName, CursorParameter[] parameters, SqlQueryExpression query) {
			DeclareCursor(request, cursorName, parameters, CursorFlags.Insensitive, query);
		}

		public static void DeclareCursor(this IRequest request, string cursorName, CursorParameter[] parameters, CursorFlags flags, SqlQueryExpression query) {
			request.ExecuteStatement(new DeclareCursorStatement(cursorName, parameters, flags, query));
		}

		#endregion

		#region Open

		public static void Open(this IRequest request, string cursorName, params SqlExpression[] args) {
			request.ExecuteStatement(new OpenStatement(cursorName, args));
		}

		#endregion

		#region Close

		public static void Close(this IRequest request, string cursorName) {
			request.ExecuteStatement(new CloseStatement(cursorName));
		}

		#endregion

		#region Fetch

		public static Row Fetch(this IRequest request, string cursorName, FetchDirection direction) {
			return Fetch(request, cursorName, direction, null);
		}

		public static Row Fetch(this IRequest request, string cursorName, FetchDirection direction, SqlExpression offset) {
			var result = request.ExecuteStatement(new FetchStatement(cursorName, direction, offset));

			if (result.RowCount > 1)
				throw new InvalidOperationException();

			return result.GetRow(0);
		}

		public static Row FetchNext(this IRequest request, string cursorName) {
			return request.Fetch(cursorName, FetchDirection.Next);
		}

		public static Row FetchPrior(this IRequest request, string cursorName) {
			return request.Fetch(cursorName, FetchDirection.Prior);
		}

		public static Row FetchFirst(this IRequest request, string cursorName) {
			return request.Fetch(cursorName, FetchDirection.First);
		}

		public static Row FetchLast(this IRequest request, string cursorName) {
			return request.Fetch(cursorName, FetchDirection.Last);
		}

		public static Row FetchRelative(this IRequest request, string cursorName, SqlExpression offset) {
			return request.Fetch(cursorName, FetchDirection.Relative, offset);
		}

		public static Row FetchAbsolute(this IRequest request, string cursorName, SqlExpression offset) {
			return request.Fetch(cursorName, FetchDirection.Absolute, offset);
		}

		#endregion

		#region Fetch Into

		// TODO:

		#endregion

		#region Declare Variable

		public static void DeclareVariable(this IRequest request, string varName, SqlType varType) {
			DeclareVariable(request, varName, varType, null);
		}

		public static void DeclareVariable(this IRequest request, string varName, SqlType varType, SqlExpression defaultExpression) {
			DeclareVariable(request, varName, varType, false, defaultExpression);
		}

		public static void DeclareVariable(this IRequest request, string varName, SqlType varType, bool notNull) {
			DeclareVariable(request, varName, varType, notNull, null);
		}

		public static void DeclareVariable(this IRequest request, string varName, SqlType varType, bool notNull, SqlExpression defaultExpression) {
			DeclareVariable(request, varName, varType, notNull, false, defaultExpression);
		}

		public static void DeclareVariable(this IRequest request, string varName, SqlType varType, bool notNull, bool constant, SqlExpression defaultExpression) {
			var statement = new DeclareVariableStatement(varName, varType) {
				IsNotNull = notNull,
				IsConstant = constant,
				DefaultExpression = defaultExpression
			};

			request.ExecuteStatement(statement);
		}

		public static void DeclareConstantVariable(this IRequest request, string varName, SqlType varType, SqlExpression defaultExpression) {
			request.DeclareVariable(varName, varType, true, true, defaultExpression);
		}

		#endregion

		#region Insert

		public static int Insert(this IRequest request, ObjectName tableName, SqlExpression[][] values) {
			return request.Insert(tableName, new string[0], values);
		}

		public static int Insert(this IRequest request, ObjectName tableName, string[] columnNames, SqlExpression[][] values) {
			return GetResult(request.ExecuteStatement(new InsertStatement(tableName, columnNames, values)));
		}

		public static int Insert(this IRequest request, ObjectName tableName, SqlExpression[] values) {
			return Insert(request, tableName, new string[0], values);
		}

		public static int Insert(this IRequest request, ObjectName tableName, string[] columnNames, SqlExpression[] values) {
			return request.Insert(tableName, columnNames, new SqlExpression[][] { values});
		}

		#endregion

		#region Update

		public static int Update(this IRequest request, ObjectName tableName, SqlExpression where, params SqlColumnAssignment[] assignments) {
			return GetResult(request.ExecuteStatement(new UpdateStatement(tableName, where, assignments)));
		}

		#endregion

		#region Select

		// TODO: instead of returning a ITable we must return a Cursor
		public static ITable Select(this IRequest request, SqlQueryExpression query, params SortColumn[] orderBy) {
			return Select(request, query, null, orderBy);
		}

		public static ITable Select(this IRequest request, SqlQueryExpression query, QueryLimit limit, params SortColumn[] orderBy) {
			return request.ExecuteStatement(new SelectStatement(query, limit, orderBy));
		}

		#endregion

		#endregion
	}
}
