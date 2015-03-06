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

using Deveel.Data.Sql.Compile;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	class StatementVisitor : SqlNodeVisitor {
		public ICollection<StatementTree> Statements { get; private set; }

		public StatementVisitor() {
			Statements = new List<StatementTree>();
		}

		private SqlExpression VisitExpression(IExpressionNode node) {
			var visitor = new ExpressionConvertVisitor(node);
			return visitor.Convert();
		}

		public void VisitSequence(StatementSequenceNode sequence) {
			foreach (var statement in sequence.Statements) {
				Visit(statement);
			}
		}

		public void Visit(IStatementNode node) {
			StatementTree statement;
			if (node is SelectStatementNode) {
				statement = VisitSelect((SelectStatementNode) node);
			} else {
				throw new NotSupportedException(String.Format("The SQL Node {0} is not a valid statement", node.GetType()));
			}

			Statements.Add(statement);
		}

		private StatementTree VisitSelect(SelectStatementNode node) {
			var queryExpression = (SqlQueryExpression) VisitExpression(node.QueryExpression);
			var tree = new StatementTree(typeof (SelectStatement));
			tree.SetValue("QueryExpression", queryExpression);
			return tree;
		}

		protected override void VisitNode(ISqlNode node) {
			if (node is StatementSequenceNode)
				VisitSequence((StatementSequenceNode)node);
			if (node is IStatementNode)
				Visit((IStatementNode)node);
		}
	}
}