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

namespace Deveel.Data.Query {
	/// <summary>
	/// An abstract class that represents an expression to be added 
	/// into a plan.
	/// </summary>
	/// <remarks>
	/// Many sets of expressions can be added into the plan tree in 
	/// any order, however it is often desirable to add some more 
	/// intensive expressions higher up the branches. This object 
	/// allows us to order expressions by optimization value. More 
	/// optimizable expressions are put near the leafs of the plan 
	/// tree and least optimizable and put near the top.
	/// </remarks>
	abstract class ExpressionPlan : IComparable {
		/// <summary>
		/// Gets or sets the optimizable value for the plan.
		/// </summary>
		public float OptimizableValue { get; set; }

		/// <summary>
		/// Adds this expression into the plan tree.
		/// </summary>
		public abstract void AddToPlanTree();

		public int CompareTo(object ob) {
			ExpressionPlan other = (ExpressionPlan)ob;
			float otherValue = other.OptimizableValue;
			if (OptimizableValue > otherValue)
				return 1;
			if (OptimizableValue < otherValue)
				return -1;
			return 0;
		}
	}
}