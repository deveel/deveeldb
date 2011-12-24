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

namespace Deveel.Data.QueryPlanning {
	/// <summary>
	/// The node for evaluating a simple pattern search on a table which
	/// includes a single left hand variable or constant, a pattern type 
	/// (<i>LIKE</i>, <i>NOT LIKE</i> or <i>REGEXP</i>), and a right hand 
	/// constant (eg. <c>T__y</c>).
	/// </summary>
	/// <remarks>
	/// If the expression is not in the form described above then this 
	/// node will not operate correctly.
	/// </remarks>
	[Serializable]
	public class SimplePatternSelectNode : SingleQueryPlanNode {
		/// <summary>
		/// The search expression.
		/// </summary>
		private Expression expression;

		public SimplePatternSelectNode(IQueryPlanNode child, Expression expression)
			: base(child) {
			this.expression = expression;
		}

		public override Table Evaluate(IQueryContext context) {
			// Evaluate the child
			Table t = Child.Evaluate(context);
			// Perform the pattern search expression on the table.
			// Split the expression,
			Expression[] exps = expression.Split();
			VariableName lhsVar = exps[0].AsVariableName();
			if (lhsVar != null) {
				// LHS is a simple variable so do a simple select
				Operator op = (Operator)expression.Last;
				return t.SimpleSelect(context, lhsVar, op, exps[1]);
			}
			// LHS must be a constant so we can just evaluate the expression
			// and see if we get true, false, null, etc.
			TObject v = expression.Evaluate(null, context);
			// If it evaluates to NULL or FALSE then return an empty set
			if (v.IsNull || v.Object.Equals(false))
				return t.EmptySelect();
			return t;
		}

		public override IList<TableName> DiscoverTableNames(IList<TableName> list) {
			return expression.DiscoverTableNames(base.DiscoverTableNames(list));
		}

		public override IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
			return expression.DiscoverCorrelatedVariables(ref level,
					 base.DiscoverCorrelatedVariables(level, list));
		}

		public override Object Clone() {
			SimplePatternSelectNode node = (SimplePatternSelectNode)base.Clone();
			node.expression = (Expression)expression.Clone();
			return node;
		}

		public override string Title {
			get { return "PATTERN: " + expression; }
		}
	}
}