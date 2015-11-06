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
using System.Linq.Expressions;

namespace Deveel.Data.Linq {
	internal class QueryBuilder : ExpressionVisitor {
		private readonly TableQuery resultQuery;

		public QueryBuilder() {
			resultQuery = new TableQuery();
		}

		public TableQuery Build(Expression expression) {
			Visit(expression);
			return resultQuery;
		}

		public TableQuery Build() {
			return Build(null);
		}

		protected override Expression VisitBinary(BinaryExpression b) {
			var expressionType = b.NodeType;
			return base.VisitBinary(b);
		}

		protected override MemberAssignment VisitMemberAssignment(MemberAssignment assignment) {
			return base.VisitMemberAssignment(assignment);
		}
	}
}
