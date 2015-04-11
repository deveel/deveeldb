using System;
using System.Linq;
using System.Linq.Expressions;

using Deveel.Data.Sql;

namespace Deveel.Data.Linq {
	class ExpressionTreeModifier : ExpressionVisitor {
		private IQueryable queryableContents;
		private readonly Type queryType;

		internal ExpressionTreeModifier(IQueryable contents, Type elementType) {
			queryableContents = contents;
			queryType = typeof (QueryableTable<>).MakeGenericType(elementType);
		}

		protected override Expression VisitConstant(ConstantExpression c) {
			if (c.Type == queryType)
				return Expression.Constant(queryableContents);

			return c;
		}
	}
}
