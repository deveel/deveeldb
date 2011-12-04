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

namespace Deveel.Data.QueryPlanning {
	/// <summary>
	/// A branch node for performing a composite function on two child nodes.
	/// </summary>
	/// <remarks>
	/// This branch is used for general <see cref="CompositeFunction.Union"/>, 
	/// <see cref="CompositeFunction.Except"/>, <see cref="CompositeFunction.Intersect"/>
	/// composites. The left and right branch results must have the same number of 
	/// columns and column types.
	/// </remarks>
	[Serializable]
	public class CompositeNode : BranchQueryPlanNode {
		/// <summary>
		/// The composite operation.
		/// </summary>
		private readonly CompositeFunction compositeOp;

		/// <summary>
		/// If this is true, the composite includes all results from both 
		/// children, otherwise removes deplicates.
		/// </summary>
		private readonly bool allOp;

		public CompositeNode(IQueryPlanNode left, IQueryPlanNode right, CompositeFunction compositeOp, bool allOp)
			: base(left, right) {
			this.compositeOp = compositeOp;
			this.allOp = allOp;
		}

		public override Table Evaluate(IQueryContext context) {
			// Solve the left branch result
			Table leftResult = Left.Evaluate(context);
			// Solve the right branch result
			Table rightResult = Right.Evaluate(context);

			// Form the composite table
			CompositeTable t = new CompositeTable(leftResult, new Table[] { leftResult, rightResult });
			t.SetupIndexesForCompositeFunction(compositeOp, allOp);
			return t;
		}

	}
}