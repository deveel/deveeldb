// 
//  Copyright 2010-2014 Deveel
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

using System;

using Deveel.Data.Protocol;
using Deveel.Data.Security;

namespace Deveel.Data.DbSystem {
	public static class DatabaseExtensions {
		public static User AuthenticateUser(this IDatabase database, string userName, string password, ConnectionEndPoint endPoint) {
			return database.UserManager.AuthenticateUser(userName, password, endPoint);
		}

		public static bool UserExists(this IDatabase database, IQueryContext context, string userName) {
			return database.UserManager.UserExists(context, userName);
		}

		public static void CreateUser(this IDatabase database, IQueryContext context, string userName, string password) {
			database.UserManager.CreateUser(context, userName, password);
		}

		public static void DeleteUser(this IDatabase database, IQueryContext context, string userName) {
			database.UserManager.DeleteUser(context, userName);
		}

		public static void DeleteAllUserGroups(this IDatabase database, IQueryContext context, string userName) {
			database.UserManager.DeleteAllUserGroups(context, userName);
		}

		public static void AlterUserPassword(this IDatabase database, IQueryContext context, string userName, string password) {
			database.UserManager.AlterUserPassword(context, userName, password);
		}

		public static void AddUserToGroup(this IDatabase database, IQueryContext context, string userName, string groupName) {
			database.UserManager.AddUserToGroup(context, userName, groupName);
		}

		public static bool UserBelongsToGroup(this IDatabase database, IQueryContext context, string userName, string groupName) {
			return database.UserManager.UserBelongsToGroup(context, userName, groupName);
		}

		public static void SetUserLock(this IDatabase database, IQueryContext context, bool lockStatus) {
			database.UserManager.SetUserLock(context, lockStatus);
		}

		public static void GrantHostAccessToUser(this IDatabase database, IQueryContext context, string userName, string protocol, string host) {
			database.UserManager.GrantHostAccessToUser(context, userName, protocol, host);
		}

		public static bool CanUserDropSequenceObject(this IDatabase database, IQueryContext context, TableName table) {
			return database.UserManager.CanUserDropSequenceObject(context, table);
		}

		public static bool CanUserCreateSequenceObject(this IDatabase database, IQueryContext context, TableName table) {
			return database.UserManager.CanUserCreateSequenceObject(context, table);
		}

		public static bool CanUserDropProcedureObject(this IDatabase database, IQueryContext context, TableName table) {
			return database.UserManager.CanUserDropProcedureObject(context, table);
		}

		public static bool CanUserCreateProcedureObject(this IDatabase database, IQueryContext context, TableName table) {
			return database.UserManager.CanUserCreateProcedureObject(context, table);
		}

		public static bool CanUserCompactTableObject(this IDatabase database, IQueryContext context, TableName table) {
			return database.UserManager.CanUserCompactTableObject(context, table);
		}

		public static bool CanUserDeleteFromTableObject(this IDatabase database, IQueryContext context, TableName table) {
			return database.UserManager.CanUserDeleteFromTableObject(context, table);
		}

		public static bool CanUserUpdateTableObject(this IDatabase database, IQueryContext context, TableName table, VariableName[] columns) {
			return database.UserManager.CanUserUpdateTableObject(context, table, columns);
		}

		public static bool CanUserInsertIntoTableObject(this IDatabase database, IQueryContext context, TableName table, VariableName[] columns) {
			return database.UserManager.CanUserInsertIntoTableObject(context, table, columns);
		}

		public static bool CanUserSelectFromTableObject(this IDatabase database, IQueryContext context, TableName table, VariableName[] columns) {
			return database.UserManager.CanUserSelectFromTableObject(context, table, columns);
		}

		public static bool CanUserDropTableObject(this IDatabase database, IQueryContext context, TableName table) {
			return database.UserManager.CanUserDropTableObject(context, table);
		}

		public static bool CanUserAlterTableObject(this IDatabase database, IQueryContext context, TableName table) {
			return database.UserManager.CanUserAlterTableObject(context, table);
		}

		public static bool CanUserCreateTableObject(this IDatabase database, IQueryContext context, TableName table) {
			return database.UserManager.CanUserCreateTableObject(context, table);
		}

		public static bool CanUserExecuteStoredProcedure(this IDatabase database, IQueryContext context, string procedureName) {
			return database.UserManager.CanUserExecuteStoredProcedure(context, procedureName);
		}

		public static bool CanUserCreateAndDropSchema(this IDatabase database, IQueryContext context, string schema) {
			return database.UserManager.CanUserCreateAndDropSchema(context, schema);
		}

		public static bool CanUserCreateAndDropUsers(this IDatabase database, IQueryContext context) {
			return database.UserManager.CanUserCreateAndDropUsers(context);
		}

		public static bool CanUserShutDown(this IDatabase database, IQueryContext context) {
			return database.UserManager.CanUserShutDown(context);
		}
	}
}