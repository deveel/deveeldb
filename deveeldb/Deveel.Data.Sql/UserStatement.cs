//  
//  UserStatement.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Handler for User commands for creating, altering and dropping user 
	/// accounts in the database.
	/// </summary>
	public class UserStatement : Statement {
		/// <summary>
		/// Private method that sets the user groups and lock status.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="username"></param>
		/// <param name="groups_list"></param>
		/// <param name="lock_status"></param>
		private void InternalSetUserGroupsAndLock(DatabaseQueryContext context, String username, Expression[] groups_list, String lock_status) {
			Database db = context.Database;

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
			if (lock_status != null) {
				if (lock_status.Equals("LOCK")) {
					db.SetUserLock(context, User, true);
				} else {
					db.SetUserLock(context, User, false);
				}
			}

		}

		/// <summary>
		/// Private method that creates a new user.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="username"></param>
		/// <param name="password_str"></param>
		/// <param name="groups_list"></param>
		/// <param name="lock_status"></param>
		private void InternalCreateUser(DatabaseQueryContext context, String username, String password_str, Expression[] groups_list, String lock_status) {
			// Create the user
			Database db = context.Database;
			db.CreateUser(context, username, password_str);

			InternalSetUserGroupsAndLock(context, username, groups_list, lock_status);

			// Allow all localhost TCP connections.
			// NOTE: Permissive initial security!
			db.GrantHostAccessToUser(context, username, "TCP", "%");
			// Allow all Local connections (from within JVM).
			db.GrantHostAccessToUser(context, username, "Local", "%");

		}

		// ---------- Implemented from Statement ----------

		internal override void Prepare() {
			// Nothing to do here
		}

		internal override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			String command_type = GetString("type");
			String username = GetString("username");

			// True if current user is altering their own user record.
			bool modify_own_record = command_type.Equals("ALTER USER") &&
										User.UserName.Equals(username);
			// True if current user is allowed to create and drop users.
			bool secure_access_privs = context.Database.CanUserCreateAndDropUsers(context, User);

			// Does the user have permissions to do this?  They must be part of the
			// 'secure access' priv group or they are modifying there own record.
			if (!(modify_own_record || secure_access_privs)) {
				throw new DatabaseException("User is not permitted to create, alter or drop user.");
			}

			if (String.Compare(username, "public", true) == 0)
				throw new DatabaseException("Username 'public' is reserved.");

			// Are we creating a new user?
			if (command_type.Equals("CREATE USER") ||
				command_type.Equals("ALTER USER")) {

				Expression password = GetExpression("password_expression");
				Expression[] groups_list = (Expression[])GetValue("groups_list");
				String lock_status = GetString("lock_status");

				String password_str = null;
				if (password != null) {
					TObject passwd_tob = password.Evaluate(null, null, context);
					password_str = passwd_tob.Object.ToString();
				}

				if (command_type.Equals("CREATE USER")) {
					// -- Creating a new user ---

					// First try and create the new user,
					Database db = context.Database;
					if (!db.UserExists(context, username)) {
						InternalCreateUser(context, username, password_str,
										   groups_list, lock_status);
					} else {
						throw new DatabaseException("User '" + username + "' already exists.");
					}

				} else if (command_type.Equals("ALTER USER")) {
					// -- Altering a user --

					// If we don't have secure access privs then we need to check that the
					// user is permitted to change the groups_list and lock_status.
					// Altering your own password is allowed, but you can't change the
					// groups you belong to, etc.
					if (!secure_access_privs) {
						if (groups_list != null) {
							throw new DatabaseException("User is not permitted to alter user groups.");
						}
						if (lock_status != null) {
							throw new DatabaseException("User is not permitted to alter user lock status.");
						}
					}

					Database db = context.Database;
					if (db.UserExists(context, username)) {
						if (password_str != null) {
							db.AlterUserPassword(context, username, password_str);
						}
						InternalSetUserGroupsAndLock(context, username, groups_list, lock_status);
					} else {
						throw new DatabaseException("User '" + username + "' doesn't exist.");
					}
				}

			} else if (command_type.Equals("DROP USER")) {
				Database db = context.Database;
				if (db.UserExists(context, username)) {
					// Delete the user
					db.DeleteUser(context, username);
				} else {
					throw new DatabaseException("User '" + username + "' doesn't exist.");
				}
			} else {
				throw new DatabaseException("Unknown user manager command: " + command_type);
			}

			return FunctionTable.ResultTable(context, 0);
		}
	}
}