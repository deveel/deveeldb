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

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class AlterUserStatement : Statement {
		private void InternalSetUserGroupsAndLock(IQueryContext context, string username, Expression[] groupsList, string lockStatus) {
			IDatabase db = context.Connection.Database;

			// Add the user to any groups
			if (groupsList != null) {
				// Delete all the groups the user currently belongs to
				db.DeleteAllUserGroups(context, username);
				for (int i = 0; i < groupsList.Length; ++i) {
					TObject group_tob = groupsList[i].Evaluate(null, null, context);
					String group_str = group_tob.Object.ToString();
					db.AddUserToGroup(context, username, group_str);
				}
			}

			// Do we lock this user?
			if (lockStatus != null) {
				if (lockStatus.Equals("LOCK")) {
					db.SetUserLock(context, true);
				} else {
					db.SetUserLock(context, false);
				}
			}

		}

		protected override Table Evaluate(IQueryContext context) {
			string username = GetString("username");

			// True if current user is altering their own user record.
			bool modifyOwnRecord = context.UserName.Equals(username);
			// True if current user is allowed to create and drop users.
			bool secureAccessPrivs = context.Connection.Database.CanUserCreateAndDropUsers(context);

			// Does the user have permissions to do this?  They must be part of the
			// 'secure access' priv group or they are modifying there own record.
			if (!(modifyOwnRecord || secureAccessPrivs))
				throw new DatabaseException("User is not permitted to create, alter or drop user.");

			if (String.Compare(username, "public", true) == 0)
				throw new DatabaseException("Username 'public' is reserved.");

			Expression password = GetExpression("password_expression");
			Expression[] groupsList = (Expression[]) GetValue("groups_list");
			String lockStatus = GetString("lock_status");

			string passwordStr = null;
			if (password != null) {
				TObject passwdTob = password.Evaluate(null, null, context);
				passwordStr = passwdTob.Object.ToString();
			}

			// -- Altering a user --

			// If we don't have secure access privs then we need to check that the
			// user is permitted to change the groups_list and lock_status.
			// Altering your own password is allowed, but you can't change the
			// groups you belong to, etc.
			if (!secureAccessPrivs) {
				if (groupsList != null)
					throw new DatabaseException("User is not permitted to alter user groups.");
				if (lockStatus != null)
					throw new DatabaseException("User is not permitted to alter user lock status.");
			}

			IDatabase db = context.Connection.Database;
			if (db.UserExists(context, username)) {
				if (passwordStr != null) {
					db.AlterUserPassword(context, username, passwordStr);
				}
				InternalSetUserGroupsAndLock(context, username, groupsList, lockStatus);
			} else {
				throw new DatabaseException("User '" + username + "' doesn't exist.");
			}

			return FunctionTable.ResultTable(context, 0);
		}
	}
}