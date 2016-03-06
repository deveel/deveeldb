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
using System.Collections.Generic;
using System.Runtime.Serialization;

using Deveel.Data;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Query {
	/// <summary>
	/// A branch node for naturally joining two tables together.
	/// </summary>
	/// <remarks>
	/// These branches should be optimized out if possible because they 
	/// result in huge results.
	/// </remarks>
	[Serializable]
	class NaturalJoinNode : BranchQueryPlanNode {
		public NaturalJoinNode(IQueryPlanNode left, IQueryPlanNode right)
			: base(left, right) {
		}

		private NaturalJoinNode(SerializationInfo info, StreamingContext context)
			: base(info, context) {
		}

		public override ITable Evaluate(IRequest context) {
			// Solve the left branch result
			var leftResult = Left.Evaluate(context);
			// Solve the Join (natural)
			return leftResult.NaturalJoin(Right.Evaluate(context));
		}
	}
}