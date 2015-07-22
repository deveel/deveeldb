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

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	public sealed class SelectStatementNode : SqlNode, IStatementNode {
		internal SelectStatementNode() {	
		}

		public SqlQueryExpressionNode QueryExpression { get; internal set; }

		/// <summary>
		/// Gets a read-oly list of <see cref="OrderBy">order</see> criteria
		/// for sorting the results of the query.
		/// </summary>
		/// <seealso cref="OrderByNode"/>
		public IEnumerable<OrderByNode> OrderBy { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "sql_query_expression") {
				QueryExpression = node as SqlQueryExpressionNode;
			} else if (node.NodeName == "order_opt") {
				// TODO:
			}

			return base.OnChildNode(node);
		}
	}
}