using System;

namespace Deveel.Data.Linq.Expressions {
	public abstract class AliasedExpression : QueryExpression {
		protected AliasedExpression(QueryExpressionType nodeType, Type type, Alias alias) 
			: base(nodeType, type) {
			Alias = alias;
		}

		public Alias Alias { get; private set; }
	}
}
