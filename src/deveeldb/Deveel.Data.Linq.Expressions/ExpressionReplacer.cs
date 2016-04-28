using System;
using System.Linq.Expressions;

namespace Deveel.Data.Linq.Expressions {
	class ExpressionReplacer : QueryExpressionVisitor {
		private Expression searchFor;
		private Expression replaceWith;

		public ExpressionReplacer(Expression searchFor, Expression replaceWith) {
			this.searchFor = searchFor;
			this.replaceWith = replaceWith;
		}

		protected override Expression Visit(Expression exp) {
			if (exp == searchFor)
				return replaceWith;

			return base.Visit(exp);
		}

		public static Expression Replace(Expression expression, Expression searchFor, Expression replaceWith) {
			return new ExpressionReplacer(searchFor, replaceWith).Visit(expression);
		}
	}
}
