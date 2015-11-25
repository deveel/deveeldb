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
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Query {
	/// <summary>
	/// A branch node for performing a composite function on two child nodes.
	/// </summary>
	/// <remarks>
	/// This branch is used for general <see cref="Sql.CompositeFunction.Union"/>, 
	/// <see cref="Sql.CompositeFunction.Except"/>, <see cref="Sql.CompositeFunction.Intersect"/>
	/// composites. The left and right branch results must have the same number of 
	/// columns and column types.
	/// </remarks>
	class CompositeNode : BranchQueryPlanNode {
		public CompositeNode(IQueryPlanNode left, IQueryPlanNode right, CompositeFunction compositeOp, bool allOp)
			: base(left, right) {
			CompositeFunction = compositeOp;
			All = allOp;
		}

		/// <summary>
		/// The composite operation.
		/// </summary>
		public CompositeFunction CompositeFunction { get; private set; }

		/// <summary>
		/// If this is true, the composite includes all results from both 
		/// children, otherwise removes deplicates.
		/// </summary>
		public bool All { get; private set; }

		public override ITable Evaluate(IRequest context) {
			var leftResult = Left.Evaluate(context);
			var rightResult = Right.Evaluate(context);

			// Form the composite table
			return leftResult.Composite(rightResult, CompositeFunction, All);
		}
	}
}