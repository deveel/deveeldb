using System;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Linq.Expressions {
	public sealed class ColumnExpression : QueryExpression, IEquatable<ColumnExpression> {
		public ColumnExpression(string name, SqlType type, Alias alias, Type nodeType)
			: base(QueryExpressionType.Column, nodeType) {
		}

		public string Name { get; private set; }

		public SqlType Type { get; private set; }

		public Alias Alias { get; private set; }

		public bool Equals(ColumnExpression other) {
			if (this == other)
				return true;

			if (!String.Equals(Name, other.Name, StringComparison.Ordinal))
				return false;

			if ((Alias == null &&
			     other.Alias == null))
				return true;

			if (Alias == null || other.Alias == null)
				return false;

			return Alias.Equals(other.Alias);
		}
	}
}
