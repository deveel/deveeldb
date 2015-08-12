using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Parser {
	class PlSqlCodeBlockNode : SqlNode {
		public IEnumerable<IStatementNode> Statements { get; private set; }

		public IEnumerable<ExceptionHandlerNode> ExceptionHandlers { get; private set; } 
 
		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName.Equals("plsql_statement_list")) {
				GetStatements(node);
			} else if (node.NodeName.Equals("exception_block_opt")) {
				ExceptionHandlers = node.FindNodes<ExceptionHandlerNode>();
			}

			return base.OnChildNode(node);
		}

		private void GetStatements(ISqlNode node) {
			var statements = new List<IStatementNode>();
			foreach (var childNode in node.ChildNodes) {
				if (childNode.NodeName.Equals("plsql_statement")) {
					statements.AddRange(childNode.ChildNodes.OfType<IStatementNode>());
				}
			}

			Statements = statements.ToArray();
		}
	}
}
