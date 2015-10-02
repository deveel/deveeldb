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
using System.IO;
using System.Linq;

using Deveel.Data;
using Deveel.Data.Security;
using Deveel.Data.Serialization;
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

			return new Prepared(UserName, action);
		}

		#region Prepared

		internal class Prepared : SqlStatement {
			public Prepared(string userName, IAlterUserAction action) {
				Action = action;
				UserName = userName;
			}

			public string UserName { get; private set; }

			public IAlterUserAction Action { get; set; }

			protected override bool IsPreparable {
				get { return false; }
			}

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

		#region PreparedSerializer

		internal class PreparedSerializer : ObjectBinarySerializer<Prepared> {
			public override void Serialize(Prepared obj, BinaryWriter writer) {
				writer.Write(obj.UserName);

				var action = obj.Action;

				writer.Write((byte)action.ActionType);

				if (action is SetPasswordAction) {
					var setPassword = (SetPasswordAction) action;
					SqlExpression.Serialize(setPassword.PasswordExpression, writer);
				} else if (action is SetAccountStatusAction) {
					var setAccount = (SetAccountStatusAction) action;
					writer.Write((byte)setAccount.Status);
				} else if (action is SetUserGroupsAction) {
					var setGroups = (SetUserGroupsAction) action;
					var groups = setGroups.Groups.ToArray();
					writer.Write(groups.Length);
					for (int i = 0; i < groups.Length; i++) {
						SqlExpression.Serialize(groups[i], writer);
					}
				} else {
					throw new NotSupportedException();
				}
			}

			public override Prepared Deserialize(BinaryReader reader) {
				var userName = reader.ReadString();

				var actionType = (AlterUserActionType) reader.ReadByte();
				IAlterUserAction action;

				if (actionType == AlterUserActionType.SetPassword) {
					var password = SqlExpression.Deserialize(reader);
					action = new SetPasswordAction(password);
				} else if (actionType == AlterUserActionType.SetGroups) {
					var groupsLength = reader.ReadInt32();
					var groups = new SqlExpression[groupsLength];
					for (int i = 0; i < groupsLength; i++) {
						groups[i] = SqlExpression.Deserialize(reader);
					}

					action = new SetUserGroupsAction(groups);
				} else if (actionType == AlterUserActionType.SetAccountStatus) {
					var status = (UserStatus) reader.ReadByte();
					action = new SetAccountStatusAction(status);
				} else {
					throw new NotSupportedException();
				}

				return new Prepared(userName, action);
			}
		}

		#endregion
	}
}
