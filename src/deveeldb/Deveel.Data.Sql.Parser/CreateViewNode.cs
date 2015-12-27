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
using System.Linq.Expressions;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class CreateViewNode : SqlStatementNode {
		public ObjectNameNode ViewName { get; private set; }

		public bool ReplaceIfExists { get; private set; }

		public IEnumerable<string> ColumnNames { get; private set; }

		public SqlQueryExpressionNode QueryExpression { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "column_list_opt") {
				GetColumnList(node);
			} else if (node.NodeName == "or_replace_opt") {
				if (node.ChildNodes.Any())
					ReplaceIfExists = true;
			} else if (node is ObjectNameNode) {
				ViewName = (ObjectNameNode) node;
			} else if (node is SqlQueryExpressionNode) {
				QueryExpression = (SqlQueryExpressionNode) node;
			}

			return base.OnChildNode(node);
		}

		private void GetColumnList(ISqlNode node) {
			var columnListNode = node.ChildNodes.FirstOrDefault();
			if (columnListNode == null)
				return;
			
			var columnNames = (columnListNode.ChildNodes.Where(childNode => childNode.NodeName.Equals("column_name"))
				.Select(childNode => childNode.ChildNodes.FirstOrDefault())
				.Where(columnName => columnName != null && columnName is IdentifierNode)
				.Select(columnName => ((IdentifierNode) columnName).Text)).ToList();

			ColumnNames = columnNames.AsEnumerable();
		}

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			var queryExpression = (SqlQueryExpression)ExpressionBuilder.Build(QueryExpression);
			var statement = new CreateViewStatement(ViewName.Name, ColumnNames, queryExpression);
			statement.ReplaceIfExists = ReplaceIfExists;
			builder.Objects.Add(statement);
		}
	}
}
