// 
//  Copyright 2010-2017 Deveel
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

using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Expressions {
	public sealed class SqlReferenceAssignExpression : SqlExpression {
		internal SqlReferenceAssignExpression(ObjectName referenceName, SqlExpression value)
			: base(SqlExpressionType.ReferenceAssign) {
			ReferenceName = referenceName ?? throw new ArgumentNullException(nameof(referenceName));
			Value = value ?? throw new ArgumentNullException(nameof(value));
		}


		public ObjectName ReferenceName { get; }

		public SqlExpression Value { get; }

		public override bool IsReference => true;

		public override SqlExpression Accept(SqlExpressionVisitor visitor) {
			return visitor.VisitReferenceAssign(this);
		}

		public override SqlType GetSqlType(QueryContext context) {
			return Value.GetSqlType(context);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			ReferenceName.AppendTo(builder);
			builder.Append(" = ");
			Value.AppendTo(builder);
		}
	}
}