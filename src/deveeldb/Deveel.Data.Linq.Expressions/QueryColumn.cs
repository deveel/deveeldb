using System;
using System.Linq.Expressions;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Linq.Expressions {
	public sealed class QueryColumn {
		public QueryColumn(string name, Expression expression, SqlType type) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");
			if (expression == null)
				throw new ArgumentNullException("expression");
			if (type == null)
				throw new ArgumentNullException("type");

			Name = name;
			Expression = expression;
			Type = type;
		}

		public string Name { get; private set; }

		public Expression Expression { get; private set; }

		public SqlType Type { get; private set; }
	}
}
