using System;

using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Triggers {
	class TriggerSystemCreateCallback : ISystemCreateCallback {
		private readonly IQuery query;

		public TriggerSystemCreateCallback(IQuery query) {
			this.query = query;
		}

		void ISystemCreateCallback.Activate(SystemCreatePhase phase) {
			if (phase == SystemCreatePhase.SystemCreate)
				Create();
		}

		private void Create() {
			var tableInfo = new TableInfo(TriggerManager.TriggerTableName);
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.Integer());
			tableInfo.AddColumn("on_object", PrimitiveTypes.String());
			tableInfo.AddColumn("action", PrimitiveTypes.Integer());
			tableInfo.AddColumn("procedure_name", PrimitiveTypes.String());
			tableInfo.AddColumn("args", PrimitiveTypes.Binary());
			tableInfo.AddColumn("body", PrimitiveTypes.Binary());
			query.Access().CreateTable(tableInfo);
		}
	}
}
