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
	public sealed class SqlQuantifyExpression : SqlExpression {
		internal SqlQuantifyExpression(SqlExpressionType expressionType, SqlBinaryExpression expression)
			: base(expressionType) {
			if (expression == null)
				throw new ArgumentNullException(nameof(expression));

			if (!expression.ExpressionType.IsRelational())
				throw new ArgumentException("Cannot quantify a non-relational expression");

			Expression = expression;
		}

		public SqlBinaryExpression Expression { get; }

		public override bool CanReduce => true;

		public override SqlExpression Accept(SqlExpressionVisitor visitor) {
			return visitor.VisitQuantify(this);
		}

		public override SqlType GetSqlType(QueryContext context) {
			return PrimitiveTypes.Boolean();
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			Expression.Left.AppendTo(builder);

			builder.Append(" ");
			builder.Append(Expression.GetOperatorString());
			builder.AppendFormat(" {0}", ExpressionType.ToString().ToUpperInvariant());

			if (Expression.Right is SqlQueryExpression)
				builder.Append("(");

			Expression.Right.AppendTo(builder);

			if (Expression.Right is SqlQueryExpression)
				builder.Append(")");
		}

		public override Task<SqlExpression> ReduceAsync(QueryContext context) {
			if (Expression.Right is SqlQueryExpression)
				return ReduceQuery(context);

			var resultType = Expression.Right.GetSqlType(context);
			if (resultType is SqlArrayType) {
				return ReduceArray(context);
			}

			throw new NotSupportedException();
		}

		private Task<SqlExpression> ReduceQuery(IContext context) {
			throw new NotImplementedException();
		}

		private async Task<SqlExpression> ReduceArray(QueryContext context) {
			var rightResult = await Expression.Right.ReduceAsync(context);
			if (!(rightResult is SqlConstantExpression))
				throw new InvalidOperationException();

			var rightValue = ((SqlConstantExpression) rightResult).Value;
			if (rightValue.IsNull)
				return Constant(SqlObject.Unknown);

			if (!(rightValue.Type is SqlArrayType))
				throw new InvalidOperationException("Invalid value for a quantification");

			var leftResult = await Expression.Left.ReduceAsync(context);
			if (!(leftResult is SqlConstantExpression))
				throw new NotSupportedException();

			var leftValue = ((SqlConstantExpression) leftResult).Value;
			var array = ((SqlArray) rightValue.Value);

			switch (ExpressionType) {
				case SqlExpressionType.Any:
					return await IsArrayAny(Expression.ExpressionType, leftValue, array, context);
				case SqlExpressionType.All:
					return await IsArrayAll(Expression.ExpressionType, leftValue, array, context);
				default:
					throw new NotSupportedException();
			}
		}

		private SqlObject Relational(SqlExpressionType opType, SqlObject a, SqlObject b) {
			switch (opType) {
				case SqlExpressionType.Equal:
					return a.Equal(b);
				case SqlExpressionType.NotEqual:
					return a.NotEqual(b);
				case SqlExpressionType.GreaterThan:
					return a.GreaterThan(b);
				case SqlExpressionType.LessThan:
					return a.LessThan(b);
				case SqlExpressionType.GreaterThanOrEqual:
					return a.GreaterThanOrEqual(b);
				case SqlExpressionType.LessThanOrEqual:
					return a.LessOrEqualThan(b);
				case SqlExpressionType.Is:
					return a.Is(b);
				case SqlExpressionType.IsNot:
					return a.IsNot(b);
				default:
					return SqlObject.Unknown;
			}
		}

		private static async Task<SqlObject> ItemValue(SqlExpression item, QueryContext context) {
			var value = await item.ReduceAsync(context);
			if (!(value is SqlConstantExpression))
				return SqlObject.Unknown;

			return ((SqlConstantExpression) value).Value;
		}

		private async Task<SqlExpression> IsArrayAll(SqlExpressionType opType, SqlObject value, SqlArray array, QueryContext context) {
			foreach (var item in array) {
				var itemValue = await ItemValue(item, context);
				var result = Relational(opType, value, itemValue);
				if (result.IsUnknown)
					return Constant(SqlObject.Unknown);

				if (result.IsFalse)
					return Constant(SqlObject.Boolean(false));
			}

			return Constant(SqlObject.Boolean(true));
		}

		private async Task<SqlExpression> IsArrayAny(SqlExpressionType opType, SqlObject value, SqlArray array, QueryContext context) {
			foreach (var item in array) {
				var itemValue = await ItemValue(item, context);
				var result = Relational(opType, value, itemValue);
				if (result.IsUnknown)
					return Constant(SqlObject.Unknown);

				if (result.IsTrue)
					return Constant(SqlObject.Boolean(true));
			}

			return Constant(SqlObject.Boolean(false));
		}
	}
}