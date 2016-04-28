using System;

using Deveel.Data.Mapping;

namespace Deveel.Data.Linq.Expressions {
	public sealed class TableExpression : AliasedExpression {
		public TableExpression(string name, Alias alias, IEntityMapping mapping)
			: base(QueryExpressionType.Table, typeof(void), alias) {
			Name = name;
			Mapping = mapping;
		}

		public string Name { get; private set; }

		public IEntityMapping Mapping { get; private set; }

		public override string ToString() {
			return String.Format("T({0})", Name);
		}
	}
}
