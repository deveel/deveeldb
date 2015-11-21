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

using Deveel.Data;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Query {
	/// <summary>
	/// The node for performing a exhaustive select operation on 
	/// the child node.
	/// </summary>
	/// <remarks>
	/// This node will iterate through the entire child result and all
	/// results that evaluate to true are included in the result.
	/// <para>
	/// <b>Note:</b> The expression may have correlated sub-queries.
	/// </para>
	/// </remarks>
	class ExhaustiveSelectNode : SingleQueryPlanNode {
		public ExhaustiveSelectNode(IQueryPlanNode child, SqlExpression exp)
			: base(child) {
			Expression = exp;
		}

		/// <summary>
		/// The search expression.
		/// </summary>
		public SqlExpression Expression { get; private set; }

		public override ITable Evaluate(IQueryContext context) {
			var t = Child.Evaluate(context);
			return t.ExhaustiveSelect(context, Expression);
		}
	}
}