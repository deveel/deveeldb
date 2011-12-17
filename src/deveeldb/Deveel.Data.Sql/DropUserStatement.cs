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
	public sealed class DropUserStatement : Statement {
		protected override Table Evaluate(IQueryContext context) {
			string username = GetString("username");

			// True if current user is allowed to create and drop users.
			bool secureAccessPrivs = context.Connection.Database.CanUserCreateAndDropUsers(context);

			// Does the user have permissions to do this?  They must be part of the
			// 'secure access' priv group
			if (!secureAccessPrivs)
				throw new DatabaseException("User is not permitted to drop user.");

			if (String.Compare(username, "public", true) == 0)
				throw new DatabaseException("Username 'public' is reserved.");

			Database db = context.Connection.Database;
			if (!db.UserExists(context, username))
				throw new DatabaseException("User '" + username + "' doesn't exist.");

			// Delete the user
			db.DeleteUser(context, username);

			return FunctionTable.ResultTable(context, 0);
		}
	}
}