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

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class ForLoop : LoopBlock {
		public ForLoop(string indexName, SqlExpression lowerBound, SqlExpression upperBound) {
			if (String.IsNullOrEmpty(indexName))
				throw new ArgumentNullException("indexName");
			if (lowerBound == null)
				throw new ArgumentNullException("lowerBound");
			if (upperBound == null)
				throw new ArgumentNullException("upperBound");

			IndexName = indexName;
			LowerBound = lowerBound;
			UpperBound = upperBound;
		}

		public string IndexName { get; private set; }

		public SqlExpression LowerBound { get; private set; }

		public SqlExpression UpperBound { get; private set; }

		public bool Reverse { get; set; }

		protected override void BeforeLoop(ExecutionContext context) {
			// TODO: define the index variable into the context

			base.BeforeLoop(context);
		}

		protected override void AfterLoop(ExecutionContext context) {
			// TODO: Get the index variable from the context
			// TODO: Increment the value of the variable
			// TODO: Redefine the value of the variable into the context

			base.AfterLoop(context);
		}

		protected override bool Loop(ExecutionContext context) {
			// TODO: Evaluate the upper and lower bound against the context
			// TODO: Evaluate the index and check it is contained within upper and lower bounds
			return base.Loop(context);
		}
	}
}
