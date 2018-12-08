// 
//  Copyright 2010-2018 Deveel
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
using System.Threading.Tasks;

using Deveel.Data.Events;
using Deveel.Data.Security;
using Deveel.Data.Sql.Statements.Security;

namespace Deveel.Data.Sql.Statements {
	public sealed class GrantObjectPrivilegesStatement : SqlStatement {
		public GrantObjectPrivilegesStatement(string grantee, Privilege privileges, ObjectName objectName, bool withGrantOption, IEnumerable<string> columns) {
			Grantee = grantee ?? throw new ArgumentNullException(nameof(grantee));

			if (privileges.IsNone || SqlPrivileges.IsSystem(privileges))
				throw new ArgumentException();

			Privileges = privileges;
			ObjectName = objectName;
			WithGrantOption = withGrantOption;
			Columns = columns;
		}

		public string Grantee { get; }

		public Privilege Privileges { get; }

		public ObjectName ObjectName { get; }

		public bool WithGrantOption { get; }

		public IEnumerable<string> Columns { get; }

		protected override SqlStatement PrepareStatement(IContext context) {
			var objName = context.QualifyName(ObjectName);
			return new GrantObjectPrivilegesStatement(Grantee, Privileges, objName, WithGrantOption, Columns);
		}

		protected override void Require(IRequirementCollection requirements) {
			requirements.Require(x => x.UserCanGrant(ObjectName, Privileges, WithGrantOption));
		}

		protected override async Task ExecuteStatementAsync(StatementContext context) {
			if (!await context.ObjectExistsAsync(ObjectName))
				throw new SqlStatementException($"The object '{ObjectName}' is not defined in this scope");

			var userManager = context.GetUserManager();
			var roleManager = context.GetRoleManager();
			var grantManager = context.GetGrantManager();

			if (await userManager.UserExistsAsync(Grantee)) {
				if (!await grantManager.GrantToUserAsync(context.User().Name, Grantee, ObjectName, Privileges, WithGrantOption))
					throw new SqlStatementException("It was not possible to grant to user because of a system error");
			} else if (await roleManager.RoleExistsAsync(Grantee)) {
				if (WithGrantOption)
					throw new SqlStatementException("Cannot set a grant option to a role");

				if (!await grantManager.GrantToRoleAsync(Grantee, ObjectName, Privileges))
					throw new SqlStatementException("It was not possible to grant to role because of a system error");
			} else {
				throw new SqlStatementException($"The grantee '{Grantee}' was not defined");
			}

			context.RaiseEvent<ObjectPrivilegesGrantedEvent>(context.User().Name, Grantee, ObjectName, Privileges, WithGrantOption);
		}
	}
}