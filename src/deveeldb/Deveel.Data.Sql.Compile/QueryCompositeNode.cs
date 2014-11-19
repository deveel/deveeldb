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

namespace Deveel.Data.Sql.Compile {
	/// <summary>
	/// Composes two queries to obtain a set that is the result of a
	/// given composition function.
	/// </summary>
	[Serializable]
	public sealed class QueryCompositeNode : SqlNode {
		/// <summary>
		/// Gets the function used to compose the two queries.
		/// </summary>
		/// <remarks>
		/// This value can be only one of <c>UNION</c>, <c>EXCEPT</c>
		/// or <c>INTERSECT</c>
		/// </remarks>
		public string CompositeFunction { get; private set; }

		/// <summary>
		/// Gets a boolean value indicating whether the composition
		/// will be done on all records.
		/// </summary>
		public bool IsAll { get; private set; }

		/// <summary>
		/// Gets the other query to compose the results.
		/// </summary>
		public SqlQueryExpressionNode QueryExpression { get; private set; }

		/// <inheritdoc/>
		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is SqlKeyNode) {

			} else if (node is SqlQueryExpressionNode) {
				
			}

			return base.OnChildNode(node);
		}
	}
}