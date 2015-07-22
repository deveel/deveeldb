using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Parser {
	public sealed class QueryUpdateNode : SqlNode {
		internal QueryUpdateNode() {
		}

		public SqlQueryExpressionNode QueryExpression { get; private set; }

		public string TableName { get; private set; }

		public IEnumerable<string> ColumnNames { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode) {
				TableName = ((ObjectNameNode) node).Name;
			} else if (node is IExpressionNode) {
				QueryExpression = (SqlQueryExpressionNode) node;
			} else if (node.NodeName == "column_list") {
				
			}

			return base.OnChildNode(node);
		}
	}
}
