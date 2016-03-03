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
using System.Linq;

using Deveel.Data.Security;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class AlterUserStatement : SqlStatement, IPreparable {
		public AlterUserStatement(string userName, IAlterUserAction alterAction) {
			if (alterAction == null)
				throw new ArgumentNullException("alterAction");
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");

			UserName = userName;
			AlterAction = alterAction;
		}

		private AlterUserStatement(ObjectData data) {
			UserName = data.GetString("UserName");
			AlterAction = data.GetValue<IAlterUserAction>("Action");
		}

		public string UserName { get; private set; }

		public IAlterUserAction AlterAction { get; private set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var action = AlterAction;
			if (action is IPreparable)
				action = ((IPreparable)action).Prepare(preparer) as IAlterUserAction;

			return new AlterUserStatement(UserName, action);
		}

		protected override void GetData(SerializeData data) {
			data.SetValue("UserName", UserName);
			data.SetValue("Action", AlterAction);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			var userName = context.Request.User().Name;

			bool modifyOwnRecord = userName.Equals(UserName);
			bool secureAccessPrivs = context.Request.Query.UserCanManageUsers();

			if (!(modifyOwnRecord || secureAccessPrivs))
				throw new MissingPrivilegesException(userName, new ObjectName(UserName), Privileges.Alter);

			if (String.Equals(UserName, "public", StringComparison.OrdinalIgnoreCase))
				throw new SecurityException("User 'public' is reserved.");

			if (AlterAction.ActionType == AlterUserActionType.SetPassword) {
				var password = ((SqlConstantExpression)((SetPasswordAction)AlterAction).PasswordExpression).Value.ToString();
				context.Request.Query.AlterUserPassword(UserName, password);
			} else if (AlterAction.ActionType == AlterUserActionType.SetGroups) {
				var groupNames = ((SetUserGroupsAction)AlterAction).Groups
					.Cast<SqlConstantExpression>()
					.Select(x => x.Value.Value.ToString())
					.ToArray();

				context.Request.Query.SetUserGroups(UserName, groupNames);
			} else if (AlterAction.ActionType == AlterUserActionType.SetAccountStatus) {
				context.Request.Query.SetUserStatus(UserName, ((SetAccountStatusAction)AlterAction).Status);
			}
		}
	}
}
