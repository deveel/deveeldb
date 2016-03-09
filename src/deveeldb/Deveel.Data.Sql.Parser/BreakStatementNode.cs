﻿// 
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
	class BreakStatementNode : SqlStatementNode {
		public string Label { get; private set; }

		public IExpressionNode WhenExpression { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName.Equals("label_opt")) {
				Label = node.FindNode<StringLiteralNode>().Value;
			} else if (node.NodeName.Equals("when_opt")) {
				WhenExpression = node.FindNode<IExpressionNode>();
			}

			return base.OnChildNode(node);
		}

		protected override void BuildStatement(SqlStatementBuilder builder) {
			SqlExpression exp = null;
			if (WhenExpression != null)
				exp = ExpressionBuilder.Build(WhenExpression);

			builder.AddObject(new BreakStatement(Label, exp));
		}
	}
}
