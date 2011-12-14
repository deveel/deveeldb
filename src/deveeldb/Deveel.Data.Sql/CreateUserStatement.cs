// 
//  Copyright 2011 Deveel
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

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class CreateUserStatement : Statement {
		private void InternalSetUserGroupsAndLock(string username, Expression[] groups_list, string lockStatus) {
			Database db = QueryContext.Database;

			// Add the user to any groups
			if (groups_list != null) {
				// Delete all the groups the user currently belongs to
				db.DeleteAllUserGroups(QueryContext, username);
				for (int i = 0; i < groups_list.Length; ++i) {
					TObject group_tob = groups_list[i].Evaluate(null, null, QueryContext);
					String group_str = group_tob.Object.ToString();
					db.AddUserToGroup(QueryContext, username, group_str);
				}
			}

			// Do we lock this user?
			if (lockStatus != null) {
				if (lockStatus.Equals("LOCK")) {
					db.SetUserLock(QueryContext, User, true);
				} else {
					db.SetUserLock(QueryContext, User, false);
				}
			}

		}

		protected override void Prepare() {
		}

		protected override Table Evaluate() {
			string username = GetString("username");
			// True if current user is allowed to create and drop users.
			bool secureAccessPrivs = QueryContext.Database.CanUserCreateAndDropUsers(QueryContext, User);

			// Does the user have permissions to do this?  They must be part of the
			// 'secure access' priv group.
			if (!secureAccessPrivs)
				throw new DatabaseException("User is not permitted to create, alter or drop user.");

			if (String.Compare(username, "public", true) == 0)
				throw new DatabaseException("Username 'public' is reserved.");

			Expression password = GetExpression("password_expression");
			Expression[] groupsList = (Expression[])GetValue("groups_list");
			string lockStatus = GetString("lock_status");

			string passwordStr = null;
			if (password != null) {
				TObject passwdTob = password.Evaluate(null, null, QueryContext);
				passwordStr = passwdTob.Object.ToString();
			}

			// First try and create the new user,
			Database db = QueryContext.Database;
			if (db.UserExists(QueryContext, username))
				throw new DatabaseException("User '" + username + "' already exists.");

			// Create the user
			db.CreateUser(QueryContext, username, passwordStr);

			InternalSetUserGroupsAndLock(username, groupsList, lockStatus);

			// Allow all localhost TCP connections.
			// NOTE: Permissive initial security!
			db.GrantHostAccessToUser(QueryContext, username, "TCP", "%");
			// Allow all Local connections .
			db.GrantHostAccessToUser(QueryContext, username, "Local", "%");


			return FunctionTable.ResultTable(QueryContext, 0);
		}
	}
}