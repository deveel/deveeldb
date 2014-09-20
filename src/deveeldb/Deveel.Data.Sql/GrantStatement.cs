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
using System.Collections;

using Deveel.Data.DbSystem;
using Deveel.Data.Security;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class GrantStatement : PrivilegesStatement {
		public GrantStatement(Privileges privileges, GrantObject grantObject, string grantName, IList users, bool grantOption)
			: base(privileges, grantObject, grantName, users, grantOption) {
		}

		public GrantStatement(Privileges privileges, GrantObject grantObject, string grantName, IList users)
			: this(privileges, grantObject, grantName, users, false) {
		}

		public GrantStatement(Privileges privileges, GrantObject grantObject, string grantName, string user, bool grantOption)
			: this(privileges, grantObject, grantName, UserList(user), grantOption) {
		}

		public GrantStatement(Privileges privileges, GrantObject grantObject, string grantName, string user)
			: this(privileges, grantObject, grantName, UserList(user), false) {
		}

		public GrantStatement() {
		}

		internal override void ExecutePrivilegeAction(IQueryContext context, PrivilegeActionInfo actionInfo) {
			GrantManager manager = context.Connection.GrantManager;

			string user = actionInfo.User;
			if (actionInfo.IsPublicUser)
				user = User.PublicName;

			// Add a user grant.
			manager.Grant(actionInfo.Privilege, actionInfo.Object, actionInfo.ObjectName, user, actionInfo.GrantOption, context.UserName);
		}
	}
}