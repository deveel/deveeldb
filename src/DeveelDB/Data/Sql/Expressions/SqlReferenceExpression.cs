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
using System.Threading.Tasks;

using Deveel.Data.Query;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Expressions {
	public sealed class SqlReferenceExpression : SqlExpression {
		internal SqlReferenceExpression(ObjectName reference)
			: base(SqlExpressionType.Reference) {
			ReferenceName = reference ?? throw new ArgumentNullException(nameof(reference));
		}

		public ObjectName ReferenceName { get; }

		public override bool IsReference => true;

		protected override void AppendTo(SqlStringBuilder builder) {
			ReferenceName.AppendTo(builder);
		}

		public override SqlExpression Accept(SqlExpressionVisitor visitor) {
			return visitor.VisitReference(this);
		}

		public override bool CanReduce => true;

		public override async Task<SqlExpression> ReduceAsync(QueryContext context) {
			if (context == null)
				throw new SqlExpressionException("A reference cannot be reduced outside a context.");

			var resolver = context.Resolver;
			if (resolver == null)
				throw new SqlExpressionException("No reference resolver was declared in this scope");

			var value = await resolver.ResolveReferenceAsync(ReferenceName);
			if (value == null)
				value = SqlObject.Unknown;

			return Constant(value);
		}

		public override SqlType GetSqlType(QueryContext context) {
			if (context == null)
				throw new SqlExpressionException("A reference cannot be reduced outside a context.");

			var resolver = context.Resolver;
			if (resolver == null)
				throw new SqlExpressionException("No reference resolver was declared in this scope");

			var value = resolver.ResolveType(ReferenceName);
			if (value == null)
				throw new SqlExpressionException();

			return value;
		}
	}
}