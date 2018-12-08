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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

using Deveel.Data.Query;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Expressions {
	/// <summary>
	/// A SQL expression that evaluates two expressions against a given operator
	/// </summary>
	public sealed class SqlBinaryExpression : SqlExpression {
		internal SqlBinaryExpression(SqlExpressionType expressionType, SqlExpression left, SqlExpression right)
			: base(expressionType) {
			if (left == null)
				throw new ArgumentNullException(nameof(left));
			if (right == null)
				throw new ArgumentNullException(nameof(right));

			Left = left;
			Right = right;
		}

		/// <summary>
		/// Gets the left as side of the expression
		/// </summary>
		public SqlExpression Left { get; }

		/// <summary>
		/// Gets the right as side of the expression
		/// </summary>
		public SqlExpression Right { get; }

		public override bool CanReduce => true;

		public override SqlType GetSqlType(QueryContext context) {
			if (ExpressionType.IsRelational())
				return PrimitiveTypes.Boolean();

			var leftType = Left.GetSqlType(context);
			var rightType = Right.GetSqlType(context);
			return leftType.Wider(rightType);
		}

		public override SqlExpression Accept(SqlExpressionVisitor visitor) {
			return visitor.VisitBinary(this);
		}

		private async Task<SqlExpression[]> ReduceSides(QueryContext context) {
			var info = new List<BinaryEvaluateInfo> {
				new BinaryEvaluateInfo {Expression = Left, Offset = 0},
				new BinaryEvaluateInfo {Expression = Right, Offset = 1}
			}.OrderByDescending(x => x.Precedence);

			foreach (var evaluateInfo in info) {
				evaluateInfo.Expression = await evaluateInfo.Expression.ReduceAsync(context);
			}

			return info.OrderBy(x => x.Offset)
				.Select(x => x.Expression)
				.ToArray();
		}

		public override async Task<SqlExpression> ReduceAsync(QueryContext context) {
			var sides = await ReduceSides(context);

			var left = sides[0];
			var right = sides[1];

			if (left.ExpressionType != SqlExpressionType.Constant)
				throw new SqlExpressionException("The reduced left side of a binary expression is not constant");
			if (right.ExpressionType != SqlExpressionType.Constant)
				throw new SqlExpressionException("The reduced right side of a binary expression is not constant.");

			var value1 = ((SqlConstantExpression)left).Value;
			var value2 = ((SqlConstantExpression)right).Value;

			var result = ReduceBinary(value1, value2);

			return Constant(result);
		}

		private SqlObject ReduceBinary(SqlObject left, SqlObject right) {
			switch (ExpressionType) {
				case SqlExpressionType.Add:
					return left.Add(right);
				case SqlExpressionType.Subtract:
					return left.Subtract(right);
				case SqlExpressionType.Multiply:
					return left.Multiply(right);
				case SqlExpressionType.Divide:
					return left.Divide(right);
				case SqlExpressionType.Modulo:
					return left.Modulo(right);
				case SqlExpressionType.GreaterThan:
					return left.GreaterThan(right);
				case SqlExpressionType.GreaterThanOrEqual:
					return left.GreaterThanOrEqual(right);
				case SqlExpressionType.LessThan:
					return left.LessThan(right);
				case SqlExpressionType.LessThanOrEqual:
					return left.LessOrEqualThan(right);
				case SqlExpressionType.Equal:
					return left.Equal(right);
				case SqlExpressionType.NotEqual:
					return left.NotEqual(right);
				case SqlExpressionType.Is:
					return left.Is(right);
				case SqlExpressionType.IsNot:
					return left.IsNot(right);
				case SqlExpressionType.And:
					return left.And(right);
				case SqlExpressionType.Or:
					return left.Or(right);
				case SqlExpressionType.XOr:
					return left.XOr(right);
				// TODO: ANY and ALL
				default:
					throw new SqlExpressionException($"The type {ExpressionType} is not a binary expression or is not supported.");
			}
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			Left.AppendTo(builder);
			builder.Append(" ");
			builder.Append(GetOperatorString());
			builder.Append(" ");
			Right.AppendTo(builder);
		}

		internal string GetOperatorString() {
			switch (ExpressionType) {
				case SqlExpressionType.Add:
					return "+";
				case SqlExpressionType.Subtract:
					return "-";
				case SqlExpressionType.Multiply:
					return "*";
				case SqlExpressionType.Modulo:
					return "%";
				case SqlExpressionType.Divide:
					return "/";
				case SqlExpressionType.Equal:
					return "=";
				case SqlExpressionType.NotEqual:
					return "<>";
				case SqlExpressionType.GreaterThan:
					return ">";
				case SqlExpressionType.GreaterThanOrEqual:
					return ">=";
				case SqlExpressionType.LessThan:
					return "<";
				case SqlExpressionType.LessThanOrEqual:
					return "<=";
				case SqlExpressionType.Or:
					return "OR";
				case SqlExpressionType.And:
					return "AND";
				case SqlExpressionType.XOr:
					return "XOR";
				case SqlExpressionType.Is:
					return "IS";
				case SqlExpressionType.IsNot:
					return "IS NOT";
				default:
					throw new SqlExpressionException($"Expression type {ExpressionType} does not have any operator string");
			}
		}

		#region BinaryEvaluateInfo

		class BinaryEvaluateInfo {
			public SqlExpression Expression { get; set; }
			public int Offset { get; set; }

			public int Precedence => Expression.Precedence;
		}

		#endregion
	}
}