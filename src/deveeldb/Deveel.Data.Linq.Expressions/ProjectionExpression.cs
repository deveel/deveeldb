using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Deveel.Data.Linq.Expressions {
	public sealed class ProjectionExpression : QueryExpression {
		public ProjectionExpression(SelectExpression source, Expression projector)
			: this(source, projector, null) {
		}

		public ProjectionExpression(SelectExpression source, Expression projector, LambdaExpression aggregate)
			: base(QueryExpressionType.Projection, aggregate != null ? aggregate.Body.Type : typeof(IEnumerable<>).MakeGenericType(projector.Type)) {
			Source = source;
			Projector = projector;
			Aggregate = aggregate;
		}

		public SelectExpression Source { get; private set; }

		public Expression Projector { get; private set; }

		public LambdaExpression Aggregate { get; private set; }

		public bool IsSingleton {
			get { return Aggregate != null && Aggregate.Body.Type == Projector.Type; }
		}
	}
}
