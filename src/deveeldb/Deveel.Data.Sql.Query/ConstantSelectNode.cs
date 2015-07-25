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
	/// <summary>
	/// The node for evaluating an expression that contains entirely 
	/// constant values (no variables).
	/// </summary>
	class ConstantSelectNode : SingleQueryPlanNode {
		public ConstantSelectNode(IQueryPlanNode child, SqlExpression expression)
			: base(child) {
			Expression = expression;
		}

		public SqlExpression Expression { get; private set; }


		public override ITable Evaluate(IQueryContext context) {
			// Evaluate the expression
			var exp = Expression.Evaluate(context, null);
			if (exp.ExpressionType != SqlExpressionType.Constant)
				throw new InvalidOperationException();

			var v = ((SqlConstantExpression) exp).Value;

			// If it evaluates to NULL or FALSE then return an empty set
			if (v.IsNull || v == false)
				return Child.Evaluate(context).EmptySelect();

			return Child.Evaluate(context);
		}
	}
}