using System;

namespace Deveel.Data.Sql.Tables {
	public interface ITableInfoBuilder {
		ITableInfoBuilder Named(string tableName);

		ITableInfoBuilder InSchema(string schemaName);

		ITableInfoBuilder WithColumn(Action<IColumnInfoBuilder> column);
	}
}
