using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Security {
	class PrivilegeManagerCreate : ISystemCreateCallback {
		private readonly IQuery query;

		public PrivilegeManagerCreate(IQuery query) {
			this.query = query;
		}

		void ISystemCreateCallback.Activate(SystemCreatePhase phase) {
			if (phase == SystemCreatePhase.SystemSetup) {
					CreateTable(query);
			}
		}

		private void CreateTable(IQuery context) {
			var tableInfo = new TableInfo(SystemSchema.GrantsTableName);
			tableInfo.AddColumn("priv_bit", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("object", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("grantee", PrimitiveTypes.String());
			tableInfo.AddColumn("grant_option", PrimitiveTypes.Boolean());
			tableInfo.AddColumn("granter", PrimitiveTypes.String());
			tableInfo = tableInfo.AsReadOnly();
			context.Access().CreateSystemTable(tableInfo);
		}
	}
}
