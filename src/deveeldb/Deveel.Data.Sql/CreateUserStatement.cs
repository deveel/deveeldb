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
using Deveel.Data.Protocol;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class CreateUserStatement : Statement {
		private void InternalSetUserGroupsAndLock(IQueryContext context, string username, Expression[] groups_list, string lockStatus) {
			IDatabase db = context.Connection.Database;

			// Add the user to any groups
			if (groups_list != null) {
				// Delete all the groups the user currently belongs to
				db.DeleteAllUserGroups(context, username);
				for (int i = 0; i < groups_list.Length; ++i) {
					TObject group_tob = groups_list[i].Evaluate(null, null, context);
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
			// True if current user is allowed to create and drop users.
			bool secureAccessPrivs = context.Connection.Database.CanUserCreateAndDropUsers(context);

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
				TObject passwdTob = password.Evaluate(null, null, context);
				passwordStr = passwdTob.Object.ToString();
			}

			// First try and create the new user,
			IDatabase db = context.Connection.Database;
			if (db.UserExists(context, username))
				throw new DatabaseException("User '" + username + "' already exists.");

			// Create the user
			db.CreateUser(context, username, passwordStr);

			InternalSetUserGroupsAndLock(context, username, groupsList, lockStatus);

			// Allow all localhost TCP connections.
			// NOTE: Permissive initial security!
			db.GrantHostAccessToUser(context, username, KnownConnectionProtocols.TcpIp, "%");
			// Allow all Local connections .
			db.GrantHostAccessToUser(context, username, KnownConnectionProtocols.Local, "%");


			return FunctionTable.ResultTable(context, 0);
		}
	}
}