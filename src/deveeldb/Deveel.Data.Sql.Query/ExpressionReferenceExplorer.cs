// 
//  Copyright 2010-2016 Deveel
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


using System.Collections.Generic;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	static class ExpressionReferenceExplorer {
		public static IEnumerable<ObjectName> AllReferences(this SqlExpression expression) {
			var discover = new ReferenceDiscover();
			discover.Visit(expression);
			return discover.References.ToArray();
		}

		#region ReferenceDiscover

		class ReferenceDiscover : SqlExpressionVisitor {
			public ReferenceDiscover() {
				References = new List<ObjectName>();
			}

			public List<ObjectName> References { get; private set; }

			public override SqlExpression VisitReference(SqlReferenceExpression reference) {
				References.Add(reference.ReferenceName);
				return base.VisitReference(reference);
			}

			public override SqlExpression VisitConstant(SqlConstantExpression constant) {
				var value = constant.Value;
				// TODO: if this is an array ...

				return base.VisitConstant(constant);
			}
		}

		#endregion
	}
}