using System;

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	class CreateViewNode : SqlNode, IStatementNode {
		public ObjectNameNode ViewName { get; private set; }

		public bool ReplaceIfExists { get; private set; }

		public SqlQueryExpressionNode QueryExpression { get; private set; }
	}
}
