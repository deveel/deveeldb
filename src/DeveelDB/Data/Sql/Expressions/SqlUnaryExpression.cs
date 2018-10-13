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

using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Expressions {
	public sealed class SqlUnaryExpression : SqlExpression {
		internal SqlUnaryExpression(SqlExpressionType expressionType, SqlExpression operand)
			: base(expressionType) {
			if (operand == null)
				throw new ArgumentNullException(nameof(operand));

			Operand = operand;
		}

		public SqlExpression Operand { get; }

		public override bool CanReduce => true;

		public override SqlExpression Accept(SqlExpressionVisitor visitor) {
			return visitor.VisitUnary(this);
		}

		public override async Task<SqlExpression> ReduceAsync(QueryContext context) {
			var operand = await Operand.ReduceAsync(context);
			if (operand.ExpressionType != SqlExpressionType.Constant)
				throw new SqlExpressionException("Operand of a unary operator could not be reduced to a constant.");

			var result = ReduceUnary(((SqlConstantExpression) operand).Value);
			return Constant(result);
		}

		private SqlObject ReduceUnary(SqlObject value) {
			switch (ExpressionType) {
				case SqlExpressionType.UnaryPlus:
					return value.Plus();
				case SqlExpressionType.Negate:
					return value.Negate();
				case SqlExpressionType.Not:
					return value.Not();
				default:
					throw new SqlExpressionException($"Expression of type {ExpressionType} is not unary.");
			}
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append(GetUnaryOperator());
			Operand.AppendTo(builder);
		}

		private string GetUnaryOperator() {
			switch (ExpressionType) {
				case SqlExpressionType.Negate:
					return "-";
				case SqlExpressionType.Not:
					return "~";
				case SqlExpressionType.UnaryPlus:
					return "+";
				default:
					throw new SqlExpressionException($"Expression type {ExpressionType} has no operator");
			}
		}

		public override SqlType GetSqlType(QueryContext context) {
			return Operand.Type;
		}
	}
}