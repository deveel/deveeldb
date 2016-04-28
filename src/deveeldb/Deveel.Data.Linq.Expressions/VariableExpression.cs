using System;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Linq.Expressions {
	public sealed class VariableExpression : QueryExpression {
		public VariableExpression(Type type, string name, SqlType varType)
			: base(QueryExpressionType.Variable, type) {
			Name = name;
			VariableType = varType;
		}

		public string Name { get; private set; }

		public SqlType VariableType { get; private set; }
	}
}
