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
	public sealed class SqlConditionExpression : SqlExpression {
		internal SqlConditionExpression(SqlExpression test, SqlExpression ifTrue, SqlExpression ifFalse)
			: base(SqlExpressionType.Condition) {
			if (test == null)
				throw new ArgumentNullException(nameof(test));
			if (ifTrue == null)
				throw new ArgumentNullException(nameof(ifTrue));

			Test = test;
			IfTrue = ifTrue;
			IfFalse = ifFalse;
		}

		public SqlExpression Test { get; }

		public SqlExpression IfTrue { get; }

		public SqlExpression IfFalse { get; }

		public override bool CanReduce => true;

		public override SqlExpression Accept(SqlExpressionVisitor visitor) {
			return visitor.VisitCondition(this);
		}

		public override SqlType GetSqlType(IContext context) {
			return IfTrue.GetSqlType(context);
		}

		public override async Task<SqlExpression> ReduceAsync(IContext context) {
			var returnType = Test.GetSqlType(context);
			if (!(returnType is SqlBooleanType))
				throw new InvalidOperationException("The expression test has not a BOOLEAN result");

			var ifTrueType = IfTrue.GetSqlType(context);
			var ifFalseType = IfFalse.GetSqlType(context);

			if (!ifTrueType.IsComparable(ifFalseType))
				throw new SqlExpressionException("The value returned in case of TRUE and in case of FALSE must be compatible");

			var testResult = await Test.ReduceAsync(context);
			if (testResult.ExpressionType != SqlExpressionType.Constant)
				throw new InvalidOperationException();

			var value = ((SqlConstantExpression) testResult).Value;
			if (value.IsNull || value.IsUnknown)
				return Constant(value);

			if (value.IsTrue)
				return await IfTrue.ReduceAsync(context);
		    if (value.IsFalse) {
                if (IfFalse != null)
		            return await IfFalse.ReduceAsync(context);

		        return Constant(SqlObject.Unknown);
		    }

		    return await base.ReduceAsync(context);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("CASE WHEN ");
			Test.AppendTo(builder);
			builder.Append(" THEN ");
			IfTrue.AppendTo(builder);
			builder.Append(" ELSE ");
			IfFalse.AppendTo(builder);
			builder.Append(" END");
		}
	}
}