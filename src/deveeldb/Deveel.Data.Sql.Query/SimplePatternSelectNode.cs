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

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	class SimplePatternSelectNode : SingleQueryPlanNode {
		public SimplePatternSelectNode(IQueryPlanNode child, SqlExpression expression) 
			: base(child) {
			Expression = expression;
		}

		public SqlExpression Expression { get; private set; }

		public override ITable Evaluate(IQueryContext context) {
			var t = Child.Evaluate(context);

			if (Expression is SqlBinaryExpression) {
				var binary = (SqlBinaryExpression) Expression;

				// Perform the pattern search expression on the table.
				// Split the expression,
				var leftRef = binary.Left.AsReferenceName();
				if (leftRef != null)
					// LHS is a simple variable so do a simple select
					return t.SimpleSelect(context, leftRef, binary.ExpressionType, binary.Right);
			}

			// LHS must be a constant so we can just evaluate the expression
			// and see if we get true, false, null, etc.
			var v = Expression.EvaluateToConstant(context, null);

			// If it evaluates to NULL or FALSE then return an empty set
			if (v.IsNull || v == false)
				return t.EmptySelect();

			return t;
		}
	}
}
