using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Statements.Build {
	public interface IColumnBuilder {
		IColumnBuilder Named(string name);

		IColumnBuilder OfType(SqlType type);

		IColumnBuilder NotNull(bool value = true);

		IColumnBuilder Identity(bool value = true);

		IColumnBuilder WithIndexType(string value);

		IColumnBuilder WithDefault(SqlExpression expression);

		IColumnBuilder WithConstraint(ColumnConstraintInfo constraintInfo);
	}
}
