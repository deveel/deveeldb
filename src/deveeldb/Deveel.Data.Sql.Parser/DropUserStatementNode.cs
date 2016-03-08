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

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class DropUserStatementNode : SqlStatementNode {
		public IEnumerable<string> UserNames { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "user_list") {
				UserNames = node.FindNodes<IdentifierNode>().Select(x => x.Text);
			}

			return base.OnChildNode(node);
		}

		protected override void BuildStatement(SqlStatementBuilder builder) {
			if (UserNames == null)
				throw new InvalidOperationException("None user was set to delete.");

			foreach (var userName in UserNames) {
				builder.AddObject(new DropUserStatement(userName));
			}
		}
	}
}