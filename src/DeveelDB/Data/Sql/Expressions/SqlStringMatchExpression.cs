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
using Deveel.Data.Text;

using Microsoft.Extensions.DependencyInjection;

namespace Deveel.Data.Sql.Expressions {
	public sealed class SqlStringMatchExpression : SqlExpression {
		internal SqlStringMatchExpression(SqlExpressionType expressionType, SqlExpression left, SqlExpression pattern,
			SqlExpression escape)
			: base(expressionType) {
			if (expressionType != SqlExpressionType.Like &&
				expressionType != SqlExpressionType.NotLike)
				throw new ArgumentException();

			if (left == null)
				throw new ArgumentNullException(nameof(left));
			if (pattern == null)
				throw new ArgumentNullException(nameof(pattern));

			Left = left;
			Pattern = pattern;
			Escape = escape;
		}

		public SqlExpression Left { get; }

		public SqlExpression Pattern { get; }

		public SqlExpression Escape { get; }

		public override SqlExpression Accept(SqlExpressionVisitor visitor) {
			return visitor.VisitStringMatch(this);
		}

		public override async Task<SqlExpression> ReduceAsync(QueryContext context) {
			var left = await Left.ReduceAsync(context);
			var pattern = await Pattern.ReduceAsync(context);

			if (left.ExpressionType != SqlExpressionType.Constant ||
				!(((SqlConstantExpression)left).Type is SqlCharacterType))
				throw new SqlExpressionException("The left expression was not reduced to a constant string");
			if (pattern.ExpressionType != SqlExpressionType.Constant ||
				!(((SqlConstantExpression)pattern).Type is SqlCharacterType))
				throw new SqlExpressionException("The pattern expression was not reduced to a constant string");

			var escapeChar = PatternSearch.EscapeCharacter;
			var leftString = (ISqlString) ((SqlConstantExpression) left).Value.Value;
			var patternString = ((SqlConstantExpression) pattern).Value.Value.ToString();

			if (Escape != null) {
				var escape = await Escape.ReduceAsync(context);
				if (escape.ExpressionType != SqlExpressionType.Constant ||
				    !(((SqlConstantExpression)escape).Type is SqlCharacterType))
					throw new SqlExpressionException("The escape expression was not reduced to a constant string");

				var s = (ISqlString) ((SqlConstantExpression) escape).Value.Value;
				if (s.Length > 1)
					throw new SqlExpressionException($"Escape string {s} too long: must be one character");

				escapeChar = s[0];
			}


			ISqlStringSearch search = null;

			if (context != null)
				search = context.GetService<ISqlStringSearch>();

			if (search == null)
				search = new SqlDefaultStringSearch();

			var result = search.Matches(leftString, patternString, escapeChar);
			if (ExpressionType == SqlExpressionType.NotLike)
				result = !result;

			return Constant(SqlObject.Boolean(result));
		}

		public override SqlType GetSqlType(QueryContext context) {
			return PrimitiveTypes.Boolean();
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			Left.AppendTo(builder);

			if (ExpressionType == SqlExpressionType.NotLike)
				builder.Append(" NOT");

			builder.Append(" LIKE ");

			Pattern.AppendTo(builder);

			if (Escape != null) {
				builder.Append(" ESCAPE ");
				Escape.AppendTo(builder);
			}
		}
	}
}