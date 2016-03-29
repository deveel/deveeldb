// 
//  Copyright 2010-2016 Deveel
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
using System.Linq;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class DeclareVariableNode : SqlStatementNode, IDeclareNode {
		public string VariableName { get; private set; }

		public DataTypeNode Type { get; private set; }

		public bool IsConstant { get; private set; }

		public bool IsNotNull { get; private set; }

		public IExpressionNode DefaultExpression { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode) {
				VariableName = ((IdentifierNode) node).Text;
			} else if (node is DataTypeNode) {
				Type = (DataTypeNode)node;
			}  else if (node.NodeName.Equals("constant_opt")) {
				IsConstant = node.ChildNodes.Any();
			} else if (node.NodeName.Equals("var_not_null_opt")) {
				IsNotNull = node.ChildNodes.Any();
			} else if (node.NodeName.Equals("var_default_opt")) {
				GetDefaultExpression(node);
			}

			return base.OnChildNode(node);
		}

		private void GetDefaultExpression(ISqlNode node) {
			foreach (var childNode in node.ChildNodes) {
				if (childNode is IExpressionNode)
					DefaultExpression = (IExpressionNode) childNode;
			}
		}

		protected override void BuildStatement(SqlStatementBuilder builder) {
			var varType = DataTypeBuilder.Build(builder.TypeResolver, Type);
			var statement = new DeclareVariableStatement(VariableName, varType);
			if (DefaultExpression != null)
				statement.DefaultExpression = ExpressionBuilder.Build(DefaultExpression);

			statement.IsConstant = IsConstant;
			statement.IsNotNull = IsConstant || IsNotNull;
			builder.AddObject(statement);
		}
	}
}
