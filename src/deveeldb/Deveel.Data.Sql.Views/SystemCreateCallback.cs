using System;

using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Views {
	class ViewsSystemCreateCallback : ISystemCreateCallback {
		private ITransaction transaction;

		public ViewsSystemCreateCallback(ITransaction transaction) {
			this.transaction = transaction;
		}

		void ISystemCreateCallback.Activate(SystemCreatePhase phase) {
			if (phase == SystemCreatePhase.SystemCreate)
				Create();
		}

		private void Create() {
			var tableInfo = new TableInfo(ViewManager.ViewTableName);
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("query", PrimitiveTypes.String());
			tableInfo.AddColumn("plan", PrimitiveTypes.Binary());

			// TODO: Columns...

			transaction.CreateTable(tableInfo);
		}
	}
}
