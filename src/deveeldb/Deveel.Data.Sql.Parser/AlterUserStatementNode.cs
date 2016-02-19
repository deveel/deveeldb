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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Security;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class AlterUserStatementNode : SqlStatementNode {
		public string UserName { get; private set; }

		public IEnumerable<IAlterUserActionNode> Actions { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode) {
				UserName = ((IdentifierNode) node).Text;
			} else if (node.NodeName.Equals("action_list")) {
				Actions = node.FindNodes<IAlterUserActionNode>();
			}

			return base.OnChildNode(node);
		}

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			var userName = UserName;

			foreach (var actionNode in Actions) {
				IAlterUserAction action;

				if (actionNode is SetPasswordNode) {
					var password = ExpressionBuilder.Build(((SetPasswordNode) actionNode).Password);
					action  = new SetPasswordAction(password);
				} else if (actionNode is SetAccountStatusNode) {
					var statusText = ((SetAccountStatusNode) actionNode).Status;
					var userStatus = ParseUserStatus(statusText);
					action = new SetAccountStatusAction(userStatus);
				} else if (actionNode is SetGroupsNode) {
					var groupNames = ((SetGroupsNode) actionNode).Groups.Select(ExpressionBuilder.Build);
					action = new SetUserGroupsAction(groupNames);
				} else {
					throw new NotSupportedException(String.Format("The action of type '{0}' is not supported.", actionNode.GetType()));
				}
				
				builder.AddObject(new AlterUserStatement(userName, action));
			}
		}

		private UserStatus ParseUserStatus(string statusText) {
			if (String.Equals(statusText, "LOCK", StringComparison.OrdinalIgnoreCase))
				return UserStatus.Locked;
			if (String.Equals(statusText, "UNLOCK", StringComparison.OrdinalIgnoreCase))
				return UserStatus.Unlocked;

			try {
				return (UserStatus) Enum.Parse(typeof (UserStatus), statusText, true);
			} catch (Exception ex) {
				throw new FormatException(String.Format("The string '{0}' is not a valid user status.", statusText), ex);
			}
		}
	}
}
