// 
//  Copyright 2010-2014 Deveel
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

using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Compile {
	/// <summary>
	/// A node in a SQL command tree that is used to invoke a function.
	/// </summary>
	[Serializable]
	public sealed class SqlFunctionCallExpressionNode : SqlNode, IExpressionNode {
		/// <summary>
		/// Gets the name of the function to be invoked.
		/// </summary>
		public ObjectName FunctionName { get; private set; }

		/// <summary>
		/// Gets an optional read-only list of arguments to be passed
		/// to the function invoked.
		/// </summary>
		public IEnumerable<IExpressionNode> Arguments { get; private set; }

		private void GetArguments(ISqlNode node) {
			var args = new List<IExpressionNode>();
			foreach (var childNode in node.ChildNodes) {
				if (childNode is IExpressionNode)
					args.Add((IExpressionNode)childNode);
			}

			Arguments = args.AsReadOnly();
		}

		/// <inheritdoc/>
		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode) {
				FunctionName = ((ObjectNameNode) node).Name;
			} else if (node.NodeName == "sql_expression_list") {
				GetArguments(node);
			}

			return base.OnChildNode(node);
		}
	}
}