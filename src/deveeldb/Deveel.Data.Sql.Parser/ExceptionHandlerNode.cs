using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Parser {
	class ExceptionHandlerNode : SqlNode {
		public bool HandlesOthers { get; private set; }

		public IEnumerable<string> HandledExceptionNames { get; private set; }

		public IEnumerable<IStatementNode> Statements { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName.Equals("handled_exceptions")) {
				GetHandledExceptions(node);
			} else if (node.NodeName.Equals("plsql_statement_list")) {
				GetStatements(node);
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

		private void GetHandledExceptions(ISqlNode node) {
			var exceptionNames = new List<string>();

			foreach (var childNode in node.ChildNodes) {
				if (HandlesOthers)
					throw new SqlParseException("Multiple exception names found when OTHERS clause was specified.");

				if (childNode is SqlKeyNode &&
				    ((SqlKeyNode) childNode).Text.Equals("OTHERS", StringComparison.OrdinalIgnoreCase)) {
					HandlesOthers = true;
				} else if (childNode is IdentifierNode) {
					exceptionNames.Add(((IdentifierNode)childNode).Text);
				}
			}

			if (!HandlesOthers)
				HandledExceptionNames = exceptionNames.ToArray();
		}
	}
}
