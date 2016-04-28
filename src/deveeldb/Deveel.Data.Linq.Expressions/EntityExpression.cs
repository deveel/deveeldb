using System;
using System.Linq.Expressions;

using Deveel.Data.Mapping;

namespace Deveel.Data.Linq.Expressions {
	public sealed class EntityExpression : QueryExpression {
		public EntityExpression(IEntityMapping entity, Expression expression)
			: base(QueryExpressionType.Entity, expression.Type) {
			Entity = entity;
			Expression = expression;
		}

		public IEntityMapping Entity { get; private set; }

		public Expression Expression { get; private set; }
	}
}
