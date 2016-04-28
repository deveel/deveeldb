using System;
using System.Linq.Expressions;

namespace Deveel.Data.Linq.Expressions {
	public static class ExpressionTypeExtensions {
		public static bool IsQueryExpression(this ExpressionType expressionType) {
			return ((int) expressionType) >= (int) QueryExpressionType.Table;
		}
	}
}
