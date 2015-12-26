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

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			if (UserNames == null)
				throw new InvalidOperationException("None user was set to delete.");

			foreach (var userName in UserNames) {
				builder.Objects.Add(new DropUserStatement(userName));
			}
		}
	}
}