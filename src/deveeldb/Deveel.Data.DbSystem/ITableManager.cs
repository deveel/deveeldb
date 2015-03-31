using System;

using Deveel.Data.Sql;

namespace Deveel.Data.DbSystem {
	public interface ITableManager : IObjectManager {
		void AssertConstraints(ObjectName tableName);

		void SelectTable(ObjectName tableName);

		void CompactTable(ObjectName tableName);

		void CreateTemporaryTable(TableInfo tableInfo);
	}
}
