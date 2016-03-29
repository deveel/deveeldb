// 
//  Copyright 2010-2016 Deveel
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
using System.Runtime.Serialization;

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

		private AlterUserStatement(SerializationInfo info, StreamingContext context) {
			UserName = info.GetString("UserName");
			AlterAction = (IAlterUserAction) info.GetValue("Action", typeof(IAlterUserAction));
		}

		public string UserName { get; private set; }

		public IAlterUserAction AlterAction { get; private set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var action = AlterAction;
			if (action is IPreparable)
				action = ((IPreparable)action).Prepare(preparer) as IAlterUserAction;

			return new AlterUserStatement(UserName, action);
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("UserName", UserName);
			info.AddValue("Action", AlterAction);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			var userName = context.User.Name;

			bool modifyOwnRecord = userName.Equals(UserName);
			bool secureAccessPrivs = context.User.CanManageUsers();

			if (!(modifyOwnRecord || secureAccessPrivs))
				throw new MissingPrivilegesException(userName, new ObjectName(UserName), Privileges.Alter);

			if (String.Equals(UserName, "public", StringComparison.OrdinalIgnoreCase))
				throw new SecurityException("User 'public' is reserved.");

			if (AlterAction.ActionType == AlterUserActionType.SetPassword) {
				var password = ((SqlConstantExpression) ((SetPasswordAction) AlterAction).PasswordExpression).Value;
				var passwordText = password.Value.ToString();
				context.DirectAccess.AlterUserPassword(UserName, passwordText);
			} else if (AlterAction.ActionType == AlterUserActionType.SetRoles) {
				var roleNames = ((SetUserRolesAction)AlterAction).Roles
					.Cast<SqlConstantExpression>()
					.Select(x => x.Value.Value.ToString())
					.ToArray();

				context.DirectAccess.SetUserRoles(UserName, roleNames);
			} else if (AlterAction.ActionType == AlterUserActionType.SetAccountStatus) {
				context.DirectAccess.SetUserStatus(UserName, ((SetAccountStatusAction)AlterAction).Status);
			}
		}
	}
}
