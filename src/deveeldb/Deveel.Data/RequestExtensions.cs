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

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data {
	public static class RequestExtensions {
		public static User User(this IRequest request) {
			return request.Query.Session.User;
		}

		public static string UserName(this IRequest request) {
			return request.User().Name;
		}

		#region Statements

		private static int GetResult(ITable table) {
			if (table.RowCount != 1)
				throw new InvalidOperationException("Invalid number of rows returned in result");
			if (table.TableInfo.ColumnCount != 1)
				throw new InvalidOperationException("Invalid number of columns returned in result");

			var value = table.GetValue(0, 0);
			return value.AsInteger();
		}

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

				var context = new ExecutionContext(request, statement);

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

		#region Assign

		public static Field Assign(this IRequest request, SqlExpression variable, SqlExpression value) {
			var result = request.ExecuteStatement(new AssignVariableStatement(variable, value));

			if (result.RowCount != 1)
				throw new InvalidOperationException();

			return result.GetValue(0, 0);
		}

		public static Field Assign(this IRequest request, string variable, SqlExpression value) {
			return request.Assign(SqlExpression.VariableReference(variable), value);
		}

		#endregion

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

		public static void FetchNextInto(this IRequest request, string cursorName, SqlExpression reference) {
			FetchInto(request, cursorName, FetchDirection.Next, reference);
		}

		public static void FetchInto(this IRequest request, string cursorName, FetchDirection direction, SqlExpression reference) {
			FetchInto(request, cursorName, direction, reference, null);
		}

		public static void FetchInto(this IRequest request, string cursorName, FetchDirection direction, SqlExpression reference, SqlExpression offset) {
			request.ExecuteStatement(new FetchIntoStatement(cursorName, direction, offset, reference));
		}

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

		#region Insert Select

		public static int InsertSelect(this IRequest request, ObjectName tableName, SqlQueryExpression query) {
			return InsertSelect(request, tableName, new string[0], query);
		}

		public static int InsertSelect(this IRequest request, ObjectName tableName, string[] columnNames, SqlQueryExpression query) {
			return GetResult(request.ExecuteStatement(new InsertSelectStatement(tableName, columnNames, query)));
		}

		#endregion

		#region Update

		public static int Update(this IRequest request, ObjectName tableName, SqlExpression where, params SqlColumnAssignment[] assignments) {
			return GetResult(request.ExecuteStatement(new UpdateStatement(tableName, where, assignments)));
		}

		#endregion

		#region Delete

		public static int Delete(this IRequest request, ObjectName tableName, SqlExpression where) {
			return Delete(request, tableName, @where, -1);
		}

		public static int Delete(this IRequest request, ObjectName tableName, SqlExpression where, int limit) {
			return GetResult(request.ExecuteStatement(new DeleteStatement(tableName, where, limit)));
		}

		#endregion

		#region Delete Current

		public static void DeleteCurrent(this IRequest request, ObjectName tableName, string cursorName) {
			request.ExecuteStatement(new DeleteCurrentStatement(tableName, cursorName));
		}

		#endregion

		#region Call

		public static void Call(this IRequest request, ObjectName procedureName, params SqlExpression[] args) {
			request.ExecuteStatement(new CallStatement(procedureName, args));
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

		#region Select Into

		// TODO: support for LIMIT clause and ORDER BY?

		public static void SelectInto(this IRequest request, SqlQueryExpression query, SqlExpression reference) {
			request.ExecuteStatement(new SelectIntoStatement(query, reference));
		}

		#endregion

		#region Show

		public static ITable Show(this IRequest request, ShowTarget target) {
			return Show(request, target, null);
		}

		public static ITable Show(this IRequest request, ShowTarget target, ObjectName objectName) {
			return request.ExecuteStatement(new ShowStatement(target, objectName));
		}

		public static ITable ShowSchema(this IRequest request) {
			return request.Show(ShowTarget.Schema);
		}

		public static ITable ShowTables(this IRequest request) {
			return request.Show(ShowTarget.SchemaTables);
		}

		public static ITable ShowStatus(this IRequest request) {
			return request.Show(ShowTarget.Status);
		}

		public static ITable ShowSession(this IRequest request) {
			return request.Show(ShowTarget.Session);
		}

		#endregion

		#endregion
	}
}
