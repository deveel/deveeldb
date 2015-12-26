using System;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	abstract class SqlStatementNode : SqlNode, IStatementNode {
		void ISqlVisitableNode.Accept(ISqlNodeVisitor visitor) {
			if (visitor is SqlCodeObjectBuilder)
				BuildStatement((SqlCodeObjectBuilder)visitor);
		}

		protected abstract void BuildStatement(SqlCodeObjectBuilder builder);
	}
}
