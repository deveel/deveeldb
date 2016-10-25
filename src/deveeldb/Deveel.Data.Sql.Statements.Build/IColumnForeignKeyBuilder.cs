using System;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements.Build {
	public interface IColumnForeignKeyBuilder {
		IColumnForeignKeyBuilder Table(ObjectName tableName);

		IColumnForeignKeyBuilder Column(string columnName);

		IColumnForeignKeyBuilder OnDelete(ForeignKeyAction action);

		IColumnForeignKeyBuilder OnUpdate(ForeignKeyAction action);
	}
}
