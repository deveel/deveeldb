using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables {
	public interface IColumnInfoBuilder {
		IColumnInfoBuilder Named(string columnName);

		IColumnInfoBuilder HavingType(SqlType columnType);

		IColumnInfoBuilder NotNull(bool value = true);

		IColumnInfoBuilder WithIndex(string indexType);

		IColumnInfoBuilder WithDefault(SqlExpression expression);
	}
}
