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

using Deveel.Data.Query;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Expressions {
	public sealed class SqlParameterExpression : SqlExpression {
		internal SqlParameterExpression()
			: base(SqlExpressionType.Parameter) {
		}

		public override SqlExpression Accept(SqlExpressionVisitor visitor) {
			return visitor.VisitParameter(this);
		}

		public override SqlType GetSqlType(QueryContext context) {
			throw new InvalidOperationException();
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("?");
		}
	}
}