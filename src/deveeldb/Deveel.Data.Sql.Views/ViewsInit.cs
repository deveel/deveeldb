using System;

using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Views {
	class ViewsInit : ITableCompositeCreateCallback {
		public void OnTableCompositeCreate(IQuery systemQuery) {
			var tableInfo = new TableInfo(ViewManager.ViewTableName);
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("query", PrimitiveTypes.String());
			tableInfo.AddColumn("plan", PrimitiveTypes.Binary());

			// TODO: Columns...

			systemQuery.Access().CreateTable(tableInfo);
		}
	}
}
