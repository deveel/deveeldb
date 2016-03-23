using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class NextSequenceValueNode : SqlStatementNode {
		public ObjectNameNode SequenceName { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode)
				SequenceName = (ObjectNameNode) node;

			return base.OnChildNode(node);
		}

		protected override void BuildStatement(SqlStatementBuilder builder) {
			var funcRef = SqlExpression.FunctionCall("NEXTVALUE", new[] {SqlExpression.Constant(SequenceName.Name)});
			var query = new SqlQueryExpression(new [] {new SelectColumn(funcRef) });
			var select = new SelectStatement(query);

			builder.AddObject(select);
		}
	}
}
