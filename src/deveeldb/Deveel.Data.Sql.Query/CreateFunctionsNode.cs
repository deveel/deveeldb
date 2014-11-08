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
using System.Text;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	/// <summary>
	/// The node for merging the child node with a set of new function 
	/// columns over the entire result.
	/// </summary>
	/// <remarks>
	/// For example, we may want to add an expression <c>a + 10</c> or 
	/// <c>coalesce(a, b, 1)</c>.
	/// </remarks>
	[Serializable]
	public sealed class CreateFunctionsNode : SingleQueryPlanNode {
		/// <summary>
		/// The list of functions to create.
		/// </summary>
		private readonly SqlExpression[] functionList;

		/// <summary>
		/// The list of names to give each function table.
		/// </summary>
		private readonly string[] nameList;

		public CreateFunctionsNode(QueryPlanNode child, SqlExpression[] functionList, string[] nameList)
			: base(child) {
			this.functionList = functionList;
			this.nameList = nameList;
		}

		public override ITable Evaluate(IQueryContext context) {
			throw new NotImplementedException();
		}

		internal override IList<ObjectName> DiscoverTableNames(IList<ObjectName> list) {
			list = base.DiscoverTableNames(list);
			foreach (SqlExpression expression in functionList) {
				list = expression.DiscoverTableNames(list);
			}
			return list;
		}

		internal override IList<QueryReference> DiscoverQueryReferences(int level, IList<QueryReference> list) {
			list = base.DiscoverQueryReferences(level, list);
			foreach (SqlExpression expression in functionList) {
				list = expression.DiscoverQueryReferences(ref level, list);
			}
			return list;
		}

		public override string Title {
			get {
				StringBuilder buf = new StringBuilder();
				buf.Append("FUNCTIONS: (");
				for (int i = 0; i < functionList.Length; ++i) {
					buf.Append(functionList[i]);
					if (i < functionList.Length - 1)
						buf.Append(", ");
				}
				buf.Append(")");
				return buf.ToString();
			}
		}
	}

}