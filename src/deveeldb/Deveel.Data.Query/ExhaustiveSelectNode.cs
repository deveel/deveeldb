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

using Deveel.Data.DbSystem;

namespace Deveel.Data.Query {
	/// <summary>
	/// The node for performing a exhaustive select operation on 
	/// the child node.
	/// </summary>
	/// <remarks>
	/// This node will iterate through the entire child result and all
	/// results that evaulate to true are included in the result.
	/// <para>
	/// <b>Note:</b> The expression may have correlated sub-queries.
	/// </para>
	/// </remarks>
	[Serializable]
	public class ExhaustiveSelectNode : SingleQueryPlanNode {
		/// <summary>
		/// The search expression.
		/// </summary>
		private Expression expression;

		public ExhaustiveSelectNode(IQueryPlanNode child, Expression exp)
			: base(child) {
			expression = exp;
		}

		public override Table Evaluate(IQueryContext context) {
			Table t = Child.Evaluate(context);
			return t.ExhaustiveSelect(context, expression);
		}

		public override IList<TableName> DiscoverTableNames(IList<TableName> list) {
			return expression.DiscoverTableNames(base.DiscoverTableNames(list));
		}

		public override IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
			return expression.DiscoverCorrelatedVariables(ref level,
					 base.DiscoverCorrelatedVariables(level, list));
		}

		public override Object Clone() {
			ExhaustiveSelectNode node = (ExhaustiveSelectNode)base.Clone();
			node.expression = (Expression)expression.Clone();
			return node;
		}

		public override string Title {
			get { return "EXHAUSTIVE: " + expression; }
		}
	}
}