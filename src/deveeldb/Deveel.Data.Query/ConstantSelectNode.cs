// 
//  Copyright 2010  Deveel
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

using Deveel.Data.DbSystem;

namespace Deveel.Data.Query {
	/// <summary>
	/// The node for evaluating an expression that contains entirely 
	/// constant values (no variables).
	/// </summary>
	[Serializable]
	public class ConstantSelectNode : SingleQueryPlanNode {
		/// <summary>
		/// The search expression.
		/// </summary>
		private Expression expression;

		public ConstantSelectNode(IQueryPlanNode child, Expression exp)
			: base(child) {
			expression = exp;
		}

		public override Table Evaluate(IQueryContext context) {
			// Evaluate the expression
			TObject v = expression.Evaluate(null, null, context);
			// If it evaluates to NULL or FALSE then return an empty set
			if (v.IsNull || v.Object.Equals(false))
				return Child.Evaluate(context).EmptySelect();
			return Child.Evaluate(context);
		}

		public override IList<TableName> DiscoverTableNames(IList<TableName> list) {
			return expression.DiscoverTableNames(base.DiscoverTableNames(list));
		}

		public override IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
			return expression.DiscoverCorrelatedVariables(ref level,
					 base.DiscoverCorrelatedVariables(level, list));
		}

		public override Object Clone() {
			ConstantSelectNode node = (ConstantSelectNode)base.Clone();
			node.expression = (Expression)expression.Clone();
			return node;
		}

		public override string Title {
			get { return "CONSTANT: " + expression; }
		}
	}
}