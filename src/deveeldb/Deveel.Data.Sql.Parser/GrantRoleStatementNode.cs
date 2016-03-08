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
	class GrantRoleStatementNode : SqlStatementNode {
		public IEnumerable<string> Grantees { get; private set; }

		public IEnumerable<string> Roles { get; private set; }

		public bool WithAdmin { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName.Equals("role_list")) {
				GetRoleList(node);
			} else if (node.NodeName.Equals("distribution_list")) {
				GetGrantees(node);
			} else if (node.NodeName.Equals("with_admin_opt")) {
				GetWithAdmin(node);
			}

			return base.OnChildNode(node);
		}

		private void GetGrantees(ISqlNode node) {
			Grantees = node.ChildNodes.OfType<IdentifierNode>().Select(x => x.Text);
		}

		private void GetRoleList(ISqlNode node) {
			Roles = node.ChildNodes.OfType<IdentifierNode>().Select(x => x.Text);
		}

		private void GetWithAdmin(ISqlNode node) {
			if (node.ChildNodes.Any())
				WithAdmin = true;
		}

		protected override void BuildStatement(SqlStatementBuilder builder) {
			foreach (var grantee in Grantees) {
				foreach (var role in Roles) {
					builder.AddObject(new GrantRoleStatement(grantee, role, WithAdmin));
				}
			}
		}
	}
}