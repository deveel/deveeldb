using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Parser {
	class InsertStatementNode : SqlNode, IStatementNode {
		public string TableName { get; private set; }

		public IEnumerable<string> ColumnNames { get; private set; }

		public ValuesInsertNode ValuesInsert { get; private set; }

		public SetInsertNode SetInsert { get; private set; }

		public QueryInsertNode QueryInsert { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode) {
				TableName = ((ObjectNameNode) node).Name;
			} else if (node.NodeName.Equals("insert_source")) {
				var colNode = node.FindByName("column_list_opt");
				if (colNode != null)
					ColumnNames = colNode.FindNodes<IdentifierNode>().Select(x => x.Text);

				ValuesInsert = node.FindNode<ValuesInsertNode>();
				SetInsert = node.FindNode<SetInsertNode>();
				QueryInsert = node.FindNode<QueryInsertNode>();
			}

			return base.OnChildNode(node);
		}
	}
}
