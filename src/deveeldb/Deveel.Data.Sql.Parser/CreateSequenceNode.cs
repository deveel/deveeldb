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
using System.Linq;
using System.Linq.Expressions;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class CreateSequenceNode : SqlStatementNode {
		public string SequenceName { get; private set; }

		public IExpressionNode IncrementBy { get; private set; }

		public IExpressionNode MinValue { get; private set; }

		public IExpressionNode MaxValue { get; private set; }

		public IExpressionNode StartWith { get; private set; }

		public bool Cycle { get; private set; }

		public IExpressionNode Cache { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode) {
				SequenceName = ((ObjectNameNode) node).Name;
			} else if (node.NodeName.Equals("start_opt")) {
				StartWith = node.FindNode<IExpressionNode>();
			} else if (node.NodeName.Equals("increment_opt")) {
				IncrementBy = node.FindNode<IExpressionNode>();
			} else if (node.NodeName.Equals("minvalue_opt")) {
				MinValue = node.FindNode<IExpressionNode>();
			} else if (node.NodeName.Equals("maxvalue_opt")) {
				MaxValue = node.FindNode<IExpressionNode>();
			} else if (node.NodeName.Equals("cycle_opt")) {
				Cycle = node.ChildNodes.Any();
			} else if (node.NodeName.Equals("cache_opt")) {
				Cache = node.FindNode<IExpressionNode>();
			}

			return base.OnChildNode(node);
		}

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			var seqName = ObjectName.Parse(SequenceName);
			var statement = new CreateSequenceStatement(seqName);

			if (IncrementBy != null)
				statement.IncrementBy = ExpressionBuilder.Build(IncrementBy);
			if (Cache != null)
				statement.Cache = ExpressionBuilder.Build(Cache);
			if (StartWith != null)
				statement.StartWith = ExpressionBuilder.Build(StartWith);
			if (MinValue != null)
				statement.MinValue = ExpressionBuilder.Build(MinValue);
			if (MaxValue != null)
				statement.MaxValue = ExpressionBuilder.Build(MaxValue);

			statement.Cycle = Cycle;

			builder.AddObject(statement);
		}
	}
}
