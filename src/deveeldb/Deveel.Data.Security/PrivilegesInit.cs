using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Security {
	class PrivilegesInit : ITableCompositeSetupCallback {
		public void OnTableCompositeSetup(IQuery systemQuery) {
			var tableInfo = new TableInfo(SystemSchema.GrantsTableName);
			tableInfo.AddColumn("priv_bit", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("object", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("grantee", PrimitiveTypes.String());
			tableInfo.AddColumn("grant_option", PrimitiveTypes.Boolean());
			tableInfo.AddColumn("granter", PrimitiveTypes.String());
			tableInfo = tableInfo.AsReadOnly();
			systemQuery.Access().CreateSystemTable(tableInfo);
		}
	}
}
