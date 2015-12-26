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

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class CreateUserStatementNode : SqlStatementNode {
		public string UserName { get; private set; }

		public IUserIdentificatorNode Identificator { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode) {
				UserName = ((IdentifierNode) node).Text;
			} else if (node.NodeName.Equals("identified")) {
				Identificator = node.FindNode<IUserIdentificatorNode>();
			}

			return base.OnChildNode(node);
		}

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			if (Identificator is IdentifiedByPasswordNode) {
				var passwordNode = (IdentifiedByPasswordNode)Identificator;
				var password = ExpressionBuilder.Build(passwordNode.Password);
				builder.Objects.Add(new CreateUserStatement(UserName, password));
			} else {
				throw new NotSupportedException();
			}
		}
	}
}