using System;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	abstract class SqlStatementNode : SqlNode, ISqlVisitableNode, IStatementNode {
		void ISqlVisitableNode.Accept(ISqlNodeVisitor visitor) {
			if (visitor is StatementBuilder)
				BuildStatement((StatementBuilder)visitor);
		}

		protected abstract void BuildStatement(StatementBuilder builder);
	}
}
