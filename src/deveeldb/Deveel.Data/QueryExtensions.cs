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
using System.Linq;

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Triggers;
using Deveel.Data.Sql.Types;

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

		#region Create Schema

		public static void CreateSchema(this IQuery query, string name) {
			query.ExecuteStatement(new CreateSchemaStatement(name));
		}

		#endregion

		#region Drop Schema

		public static void DropSchema(this IQuery query, string name) {
			query.ExecuteStatement(new DropSchemaStatement(name));
		}

		#endregion

		#region Create Table

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

		#region Alter Table

		public static void AlterTable(this IQuery query, ObjectName tableName, IAlterTableAction action) {
			query.ExecuteStatement(new AlterTableStatement(tableName, action));
		}

		public static void AddColumn(this IQuery query, ObjectName tableName, SqlTableColumn column) {
			query.AlterTable(tableName, new AddColumnAction(column));
		}

		public static void AddColumn(this IQuery query, ObjectName tableName, string columnName, SqlType columnType) {
			query.AddColumn(tableName, new SqlTableColumn(columnName, columnType));
		}

		public static void DropColumn(this IQuery query, ObjectName tableName, string columnName) {
			query.AlterTable(tableName, new DropColumnAction(columnName));
		}

		public static void AddConstraint(this IQuery query, ObjectName tableName, SqlTableConstraint constraint) {
			query.AlterTable(tableName, new AddConstraintAction(constraint));
		}

		public static void AddPrimaryKey(this IQuery query, ObjectName tableName, params string[] columnNames) {
			query.AddPrimaryKey(tableName, null, columnNames);
		}

		public static void AddPrimaryKey(this IQuery query, ObjectName tableName, string constraintName, params string[] columnNames) {
			query.AddConstraint(tableName, SqlTableConstraint.PrimaryKey(constraintName, columnNames));
		}

		public static void DropPrimaryKey(this IQuery query, ObjectName tableName) {
			query.AlterTable(tableName, new DropPrimaryKeyAction());
		}

		public static void AddUniqueKey(this IQuery query, ObjectName tableName, params string[] columnNames) {
			AddUniqueKey(query, tableName, null, columnNames);
		}

		public static void AddUniqueKey(this IQuery query, ObjectName tableName, string constraintName, params string[] columnNames) {
			query.AddConstraint(tableName, SqlTableConstraint.UniqueKey(constraintName, columnNames));
		}

		public static void DropConstraint(this IQuery query, ObjectName tableName, string constraintName) {
			query.AlterTable(tableName, new DropConstraintAction(constraintName));
		}

		public static void SetDefault(this IQuery query, ObjectName tableName, string columnName, SqlExpression expression) {
			query.AlterTable(tableName, new SetDefaultAction(columnName, expression));
		}

		public static void DropDefault(this IQuery query, ObjectName tableName, string columnName) {
			query.AlterTable(tableName, new DropDefaultAction(columnName));
		}

		#endregion

		#region Drop Table

		public static void DropTable(this IQuery query, ObjectName tableName) {
			DropTable(query, tableName, false);
		}

		public static void DropTable(this IQuery query, ObjectName tableName, bool ifExists) {
			query.ExecuteStatement(new DropTableStatement(tableName, ifExists));
		}

		#endregion

		#region Create View

		public static void CreateView(this IQuery query, ObjectName viewName, string querySource) {
			CreateView(query, viewName, new string[0], querySource);
		}

		public static void CreateView(this IQuery query, ObjectName viewName, IEnumerable<string> columnNames, string querySource) {
			var expression = SqlExpression.Parse(querySource);
			if (expression.ExpressionType != SqlExpressionType.Query)
				throw new ArgumentException("The input query string is invalid.", "querySource");

			query.CreateView(viewName, columnNames, (SqlQueryExpression)expression);
		}

		public static void CreateView(this IQuery query, ObjectName viewName, SqlQueryExpression queryExpression) {
			CreateView(query, viewName, new string[0], queryExpression);
		}

		public static void CreateView(this IQuery query, ObjectName viewName, IEnumerable<string> columnNames,
			SqlQueryExpression queryExpression) {
			query.ExecuteStatement(new CreateViewStatement(viewName, columnNames, queryExpression));
		}

		#endregion

		#region Drop View

		public static void DropView(this IQuery query, ObjectName viewName) {
			DropView(query, viewName, false);
		}

		public static void DropView(this IQuery query, ObjectName viewName, bool ifExists) {
			query.ExecuteStatement(new DropViewStatement(viewName, ifExists));
		}

		#endregion

		#region Create Group

		public static void CreateRole(this IQuery query, string roleName) {
			query.ExecuteStatement(new CreateRoleStatement(roleName));
		}

		#endregion

		#region Drop Role

		public static void DropRole(this IQuery query, string roleName) {
			query.ExecuteStatement(new DropRoleStatement(roleName));
		}

		#endregion

		#region Create User

		public static void CreateUser(this IQuery query, string userName, SqlExpression password) {
			query.ExecuteStatement(new CreateUserStatement(userName, password));
		}

		public static void CreateUser(this IQuery query, string userName, string password) {
			query.CreateUser(userName, SqlExpression.Constant(Field.VarChar(password)));
		}

		#endregion

		#region Alter User

		public static void AlterUser(this IQuery query, string userName, IAlterUserAction action) {
			query.ExecuteStatement(new AlterUserStatement(userName, action));
		}

		public static void SetPassword(this IQuery query, string userName, SqlExpression password) {
			query.AlterUser(userName, new SetPasswordAction(password));
		}

		public static void SetPassword(this IQuery query, string userName, string password) {
			query.SetPassword(userName, SqlExpression.Constant(Field.VarChar(password)));
		}

		public static void SetRoles(this IQuery query, string userName, params SqlExpression[] roleNames) {
			query.AlterUser(userName, new SetUserRolesAction(roleNames));
		}

		public static void SetRoles(this IQuery query, string userName, params string[] roleNames) {
			if (roleNames == null)
				throw new ArgumentNullException("roleNames");

			var roles = roleNames.Select(x => SqlExpression.Constant(Field.VarChar(x))).Cast<SqlExpression>().ToArray();
			query.SetRoles(userName, roles);
		}

		public static void SetAccountStatus(this IQuery query, string userName, UserStatus status) {
			query.AlterUser(userName, new SetAccountStatusAction(status));
		}

		public static void SetAccountLocked(this IQuery query, string userName) {
			query.SetAccountStatus(userName, UserStatus.Locked);
		}

		public static void SetAccountUnlocked(this IQuery query, string userName) {
			query.SetAccountStatus(userName, UserStatus.Unlocked);
		}

		#endregion

		#region Drop User

		public static void DropUser(this IQuery query, string userName) {
			query.ExecuteStatement(new DropUserStatement(userName));
		}

		#endregion


		#region Grant Privileges

		public static void Grant(this IQuery query, string grantee, Privileges privileges, ObjectName objectName) {
			query.Grant(grantee, privileges, false, objectName);
		}

		public static void Grant(this IQuery query, string grantee, Privileges privileges, bool withOption, ObjectName objectName) {
			query.ExecuteStatement(new GrantPrivilegesStatement(grantee, privileges, withOption, objectName));
		}

		#endregion

		#region Grant Role

		public static void GrantRole(this IQuery query, string userName, string roleName) {
			GrantRole(query, userName, roleName, false);
		}

		public static void GrantRole(this IQuery query, string userName, string roleName, bool withAdmin) {
			query.ExecuteStatement(new GrantRoleStatement(userName, roleName, withAdmin));
		}

		#endregion

		#region Revoke Privileges

		public static void Revoke(this IQuery query, string grantee, Privileges privileges, ObjectName objectName) {
			Revoke(query, grantee, privileges, false, objectName);
		}

		public static void Revoke(this IQuery query, string grantee, Privileges privileges, bool grantOption, ObjectName objectName) {
			query.ExecuteStatement(new RevokePrivilegesStatement(grantee, privileges, grantOption, objectName, new string[0]));
		}

		#endregion

		#region Create Trigger

		public static void CreateTrigger(this IQuery query, ObjectName triggerName, ObjectName tableName, PlSqlBlockStatement body, TriggerEventType eventType) {
			query.ExecuteStatement(new CreateTriggerStatement(triggerName, tableName, body, eventType));
		}

		public static void CreateBeforeInsertTrigger(this IQuery query, ObjectName triggerName, ObjectName tableName, PlSqlBlockStatement body) {
			query.CreateTrigger(triggerName, tableName, body, TriggerEventType.BeforeInsert);
		}

		public static void CreateAfterInsertTrigger(this IQuery query, ObjectName triggerName, ObjectName tableName, PlSqlBlockStatement body) {
			query.CreateTrigger(triggerName, tableName, body, TriggerEventType.AfterInsert);
		}

		public static void CreateBeforeUpdateTrigger(this IQuery query, ObjectName triggerName, ObjectName tableName, PlSqlBlockStatement body) {
			query.CreateTrigger(triggerName, tableName, body, TriggerEventType.BeforeUpdate);
		}

		public static void CreateAfterUpdateTrigger(this IQuery query, ObjectName triggerName, ObjectName tableName, PlSqlBlockStatement body) {
			query.CreateTrigger(triggerName, tableName, body, TriggerEventType.AfterUpdate);
		}

		public static void CreateBeforeDeleteTrigger(this IQuery query, ObjectName triggerName, ObjectName tableName, PlSqlBlockStatement body) {
			query.CreateTrigger(triggerName, tableName, body, TriggerEventType.BeforeDelete);
		}

		public static void CreateAfterDeleteTrigger(this IQuery query, ObjectName triggerName, ObjectName tableName, PlSqlBlockStatement body) {
			query.CreateTrigger(triggerName, tableName, body, TriggerEventType.AfterDelete);
		}

		public static void CreateCallbackTrigger(this IQuery query, ObjectName tableName, TriggerEventType eventType) {
			query.ExecuteStatement(new CreateCallbackTriggerStatement(tableName, eventType));
		}

		#endregion

		#region DropTrigger

		public static void DropTrigger(this IQuery query, ObjectName triggerName) {
			query.ExecuteStatement(new DropTriggerStatement(triggerName));
		}

		public static void DropCallbackTrigger(this IQuery query, ObjectName tableName) {
			query.ExecuteStatement(new DropCallbackTriggersStatement(tableName));
		}

		#endregion

		#region Create Sequence

		#endregion

		#region Drop Sequence

		public static void DropSequence(this IQuery query, ObjectName sequenceName) {
			query.ExecuteStatement(new DropSequenceStatement(sequenceName));
		}

		#endregion

		#region Show

		public static void Show(this IQuery query, ShowTarget target) {
			query.ExecuteStatement(new ShowStatement(target));
		}

		#endregion

		#region Commit

		public static void Commit(this IQuery query) {
			query.ExecuteStatement(new CommitStatement());
		}

		#endregion

		#region Rollback

		public static void Rollback(this IQuery query) {
			query.ExecuteStatement(new RollbackStatement());
		}

		#endregion

		#endregion
	}
}
