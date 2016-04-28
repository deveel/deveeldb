using System;
using System.Linq.Expressions;

namespace Deveel.Data.Linq.Expressions {
	public abstract class QueryExpression : Expression {
		protected QueryExpression(QueryExpressionType nodeType, Type type) 
			: base((ExpressionType)((int) nodeType), type) {
		}
	}
}
