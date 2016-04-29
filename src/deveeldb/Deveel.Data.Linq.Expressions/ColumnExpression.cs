using System;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Linq.Expressions {
	public sealed class ColumnExpression : QueryExpression, IEquatable<ColumnExpression> {
		public ColumnExpression(string name, SqlType sqlType, Alias alias, Type type)
			: base(QueryExpressionType.Column, type) {
			SqlType = sqlType;
			Name = name;
			Alias = alias;
		}

		public string Name { get; private set; }

		public SqlType SqlType { get; private set; }

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
