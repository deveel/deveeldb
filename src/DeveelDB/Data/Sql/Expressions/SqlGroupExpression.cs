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
using System.Runtime.Serialization;
using System.Threading.Tasks;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Expressions {
	public sealed class SqlGroupExpression : SqlExpression {
		internal SqlGroupExpression(SqlExpression expression)
			: base(SqlExpressionType.Group) {
			if (expression == null)
				throw new ArgumentNullException(nameof(expression));

			Expression = expression;
		}

		public SqlExpression Expression { get; }

		public override SqlType GetSqlType(IContext context) {
			return Expression.GetSqlType(context);
		}

		public override SqlExpression Accept(SqlExpressionVisitor visitor) {
			return visitor.VisitGroup(this);
		}

		public override Task<SqlExpression> ReduceAsync(IContext context) {
			return Expression.ReduceAsync(context);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("(");
			Expression.AppendTo(builder);
			builder.Append(")");
		}
	}
}