// 
//  Copyright 2010-2018 Deveel
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

namespace Deveel.Data.Sql.Expressions {
	class SqlExpressionPrepareVisitor : SqlExpressionVisitor {
		private readonly ISqlExpressionPreparer preparer;

		public SqlExpressionPrepareVisitor(ISqlExpressionPreparer preparer) {
			this.preparer = preparer;
		}

		// TODO: find a way to make it async
		public override SqlExpression Visit(SqlExpression expression) {
			if (preparer.CanPrepare(expression))
				expression = preparer.Prepare(expression);

			return base.Visit(expression);
		}

		public override SqlQueryExpressionFrom VisitQueryFrom(SqlQueryExpressionFrom @from) {
			var result = @from;
			if (result != null)
				result = result.Prepare(preparer);

			return result;
		}
	}
}