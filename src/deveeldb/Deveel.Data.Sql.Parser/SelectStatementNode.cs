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
	class SelectStatementNode : SqlStatementNode {
		public SqlQueryExpressionNode QueryExpression { get; private set; }

		/// <summary>
		/// Gets a read-oly list of <see cref="OrderBy">order</see> criteria
		/// for sorting the results of the query.
		/// </summary>
		/// <seealso cref="OrderByNode"/>
		public IEnumerable<OrderByNode> OrderBy { get; private set; }

		public LimitNode Limit { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "sql_query_expression") {
				QueryExpression = node as SqlQueryExpressionNode;
			} else if (node.NodeName == "order_opt") {
				GetOrderBy(node);
			} else if (node.NodeName == "limit_opt") {
				GetLimit(node);
			}

			return base.OnChildNode(node);
		}

		private void GetOrderBy(ISqlNode node) {
			var listNode =  node.FindByName("sorted_def_list");
			if (listNode != null) {
				OrderBy = listNode.ChildNodes.Cast<OrderByNode>();
			}
		}

		private void GetLimit(ISqlNode node) {
			var child = node.ChildNodes.FirstOrDefault();
			if (child != null)
				Limit = (LimitNode) child;
		}

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			var queryExpression = (SqlQueryExpression) ExpressionBuilder.Build(QueryExpression);
			if (QueryExpression.IntoClause != null) {
				var refExp = ExpressionBuilder.Build(QueryExpression.IntoClause);
				builder.AddObject(new SelectIntoStatement(queryExpression, refExp));
			} else {
				var orderBy = BuildOrderBy(OrderBy);
				var statement = new SelectStatement(queryExpression, orderBy);
				statement.Limit = BuildLimit(Limit);
				builder.AddObject(statement);
			}
		}

		private IEnumerable<SortColumn> BuildOrderBy(IEnumerable<OrderByNode> nodes) {
			if (nodes == null)
				return null;

			return nodes.Select(node => new SortColumn(ExpressionBuilder.Build(node.Expression), node.Ascending));
		}

		private QueryLimit BuildLimit(LimitNode node) {
			if (node == null)
				return null;

			return new QueryLimit(node.Offset, node.Count);
		}
	}
}