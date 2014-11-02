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
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
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
	internal class ExhaustiveSelectNode : SingleQueryPlanNode {
		public ExhaustiveSelectNode(QueryPlanNode child, SqlExpression exp)
			: base(child) {
			Expression = exp;
		}

		public override ITable Evaluate(IQueryContext context) {
			throw new NotImplementedException();
		}

		public override IList<ObjectName> DiscoverTableNames(IList<ObjectName> list) {
			throw new NotImplementedException();
		}

		public override IList<QueryReference> DiscoverQueryReferences(int level, IList<QueryReference> list) {
			throw new NotImplementedException();
		}

		public override string Title {
			get { return "EXHAUSTIVE: " + Expression; }
		}

		/// <summary>
		/// The search expression.
		/// </summary>
		public SqlExpression Expression { get; private set; }
	}
}