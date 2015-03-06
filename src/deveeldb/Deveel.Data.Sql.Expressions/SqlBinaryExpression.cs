// 
//  Copyright 2010-2015 Deveel
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
using System.Security.Policy;

using Deveel.Data.Deveel.Data.Sql;

namespace Deveel.Data.Sql.Expressions {
	[Serializable]
	public sealed class SqlBinaryExpression : SqlExpression {
		private readonly SqlExpressionType expressionType;

		internal SqlBinaryExpression(SqlExpression left, SqlExpressionType expressionType, SqlExpression right, BinaryOperator binaryOperator) {
			this.expressionType = expressionType;
			BinaryOperator = binaryOperator;

			Left = left;
			Right = right;
		}

		public SqlExpression Left { get; private set; }

		public SqlExpression Right { get; private set; }

		private DataObject EvaluateBinary(DataObject left, DataObject right, EvaluateContext context) {
			return BinaryOperator.Evaluate(left, right, context);
		}

		public override bool CanEvaluate {
			get { return true; }
		}

		public override SqlExpressionType ExpressionType {
			get { return expressionType; }
		}

		internal BinaryOperator BinaryOperator { get; private set; }

		private SqlExpression[] EvaluateSides(EvaluateContext context) {
			var info = new List<EvaluateInfo> {
				new EvaluateInfo {Expression = Left, Offset = 0},
				new EvaluateInfo {Expression = Right, Offset = 1}
			}.OrderByDescending(x => x.Precedence);

			foreach (var evaluateInfo in info) {
				evaluateInfo.Expression = evaluateInfo.Expression.Evaluate(context);
			}

			return info.OrderBy(x => x.Offset)
				.Select(x => x.Expression)
				.ToArray();
		}

		public override SqlExpression Evaluate(EvaluateContext context) {
			var sides = EvaluateSides(context);

			var leftExp = sides[0];
			var rightExp = sides[1];

			if (!(leftExp is SqlConstantExpression) ||
			    !(rightExp is SqlConstantExpression))
				throw new ExpressionEvaluateException(
					String.Format("One of the arguments of the binary expression {0} could not be evaluated to constant.",
						ExpressionType));

			var left = ((SqlConstantExpression) leftExp).Value;
			var right = ((SqlConstantExpression) rightExp).Value;
			var computedValue = EvaluateBinary(left, right, context);

			return new SqlConstantExpression(computedValue);
		}

		#region EvaluateInfo

		class EvaluateInfo {
			public SqlExpression Expression { get; set; }
			public int Offset { get; set; }

			public int Precedence {
				get { return Expression.EvaluatePrecedence; }
			}
		}

		#endregion
	}
}