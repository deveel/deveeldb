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

using Deveel.Data.DbSystem;
using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class AlterUserStatement : SqlStatement {
		public AlterUserStatement(string userName, IAlterUserAction alterAction) {
			if (alterAction == null)
				throw new ArgumentNullException("alterAction");
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");

			UserName = userName;
			AlterAction = alterAction;
		}

		public string UserName { get; private set; }

		public IAlterUserAction AlterAction { get; private set; }

		protected override SqlStatement PrepareStatement(IExpressionPreparer preparer, IQueryContext context) {
			var action = AlterAction;
			if (action is IPreparable)
				action = ((IPreparable) action).Prepare(preparer) as IAlterUserAction;
			if (action == null)
				throw new StatementPrepareException("Could not prepare the action");

			return new Prepared(this, UserName, action);
		}

		#region Prepared

		class Prepared : SqlPreparedStatement {
			public Prepared(AlterUserStatement source, string userName, IAlterUserAction action) 
				: base(source) {
				Action = action;
				UserName = userName;
			}

			public string UserName { get; private set; }

			private IAlterUserAction Action { get; set; }

			protected override ITable ExecuteStatement(IQueryContext context) {
				if (Action.ActionType == AlterUserActionType.SetPassword) {
					var password = ((SqlConstantExpression) ((SetPasswordAction) Action).PasswordExpression).Value.ToString();
					context.AlterUserPassword(UserName, password);
				} else if (Action.ActionType == AlterUserActionType.SetGroups) {
					context.RemoveUserFromAllGroups(UserName);

					var groupNames = ((SetUserGroupsAction) Action).Groups
						.Cast<SqlConstantExpression>()
						.Select(x => x.Value.Value.ToString())
						.ToArray();
					foreach (string group in groupNames) {
						context.AddUserToGroup(UserName, group);
					}
				} else if (Action.ActionType == AlterUserActionType.SetAccountStatus) {
					context.SetUserStatus(UserName, ((SetAccountStatusAction)Action).Status);
				}

				return FunctionTable.ResultTable(context, 0);
			}
		}

		#endregion
	}
}
