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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class SetVariableStatementNode : SqlStatementNode {
		public IExpressionNode VariableReference { get; private set; }

		public IExpressionNode ValueExpression { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IExpressionNode) {
				if (VariableReference == null) {
					VariableReference = (IExpressionNode) node;
				} else {
					ValueExpression = (IExpressionNode) node;
				}
			}

			return base.OnChildNode(node);
		}

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			var varRefExp = ExpressionBuilder.Build(VariableReference);
			var valueExp = ExpressionBuilder.Build(ValueExpression);

			if (!(varRefExp is SqlVariableReferenceExpression) &&
				!(varRefExp is SqlReferenceExpression))
				throw new NotSupportedException("Only simple references are supported now.");

			builder.AddObject(new AssignVariableStatement(varRefExp, valueExp));
		}
	}
}