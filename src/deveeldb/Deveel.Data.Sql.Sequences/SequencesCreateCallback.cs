using System;

using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Sequences {
	class SequencesCreateCallback : ISystemCreateCallback {
		private IQuery query;

		public SequencesCreateCallback(IQuery query) {
			this.query = query;
		}

		void ISystemCreateCallback.Activate(SystemCreatePhase phase) {
			if (phase == SystemCreatePhase.SystemCreate)
				Create();
		}

		private void Create() {
			// SYSTEM.SEQUENCE_INFO
			var tableInfo = new TableInfo(SequenceManager.SequenceInfoTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.Numeric());
			tableInfo = tableInfo.AsReadOnly();
			query.Access.CreateTable(tableInfo);

			// SYSTEM.SEQUENCE
			tableInfo = new TableInfo(SequenceManager.SequenceTableName);
			tableInfo.AddColumn("seq_id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("last_value", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("increment", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("minvalue", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("maxvalue", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("start", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("cache", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("cycle", PrimitiveTypes.Boolean());
			tableInfo = tableInfo.AsReadOnly();
			query.Access.CreateTable(tableInfo);
		}
	}
}
