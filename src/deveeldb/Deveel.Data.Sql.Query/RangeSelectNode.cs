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
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Query {
	/// <summary>
	/// The node for performing a simple indexed query on a single column 
	/// of the child node.
	/// </summary>
	/// <remarks>
	/// Finds the set from the child node that matches the range.
	/// <para>
	/// The given <see cref="Expression"/> object must conform to a number of 
	/// rules. It may reference only one column in the child node. It must 
	/// consist of only simple mathemetical and logical operators (&lt;, &gt;, 
	/// =, &lt;&gt;, &gt;=, &lt;=, AND, OR).
	/// The left side of each mathematical operator must be a variable, 
	/// and the right side must be a constant (parameter subsitution or 
	/// correlated value).
	/// </para>
	/// <para>
	/// Breaking any of these rules will mean the range select can not 
	/// happen.
	/// </para>
	/// </remarks>
	/// <example>
	/// For example:
	/// <code>
	/// (col &gt; 10 AND col &lt; 100) OR col &gt; 1000 OR col == 10
	/// </code>
	/// </example>
	[Serializable]
	class RangeSelectNode : SingleQueryPlanNode {
		public RangeSelectNode(QueryPlanNode child, SqlExpression expression)
			: base(child) {
			this.Expression = expression;
		}

		/// <inheritdoc/>
		public override ITable Evaluate(IQueryContext context) {
			throw new NotImplementedException();
		}

		/// <inheritdoc/>
		public override IList<ObjectName> DiscoverTableNames(IList<ObjectName> list) {
			throw new NotImplementedException();
		}

		/// <inheritdoc/>
		public override IList<QueryReference> DiscoverQueryReferences(int level, IList<QueryReference> list) {
			throw new NotImplementedException();
		}

		/// <inheritdoc/>
		public override string Title {
			get { return "RANGE: " + Expression; }
		}

		/// <summary>
		/// A simple expression that represents the range to select.  See the
		/// class comments for a description for how this expression must be
		/// formed.
		/// </summary>
		public SqlExpression Expression { get; private set; }
	}
}