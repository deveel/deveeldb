using System;

using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Schemas {
	class SchemaSystemCreateCallback : ISystemCreateCallback {
		private readonly IQuery query;

		public SchemaSystemCreateCallback(IQuery query) {
			this.query = query;
		}

		void ISystemCreateCallback.Activate(SystemCreatePhase phase) {
			if (phase == SystemCreatePhase.SystemCreate) {
				// SYSTEM.SCHEMA_INFO
				var tableInfo = new TableInfo(SystemSchema.SchemaInfoTableName);
				tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
				tableInfo.AddColumn("name", PrimitiveTypes.String());
				tableInfo.AddColumn("type", PrimitiveTypes.String());
				tableInfo.AddColumn("culture", PrimitiveTypes.String());
				tableInfo.AddColumn("other", PrimitiveTypes.String());
				tableInfo = tableInfo.AsReadOnly();
				query.Access.CreateTable(tableInfo);

				// TODO: Move this to the setup phase?
				CreateSystemSchema();
			}
		}

		private void CreateSchema(string name, string type) {
			query.Access.CreateSchema(new SchemaInfo(name, type));
		}

		private void CreateSystemSchema() {
			CreateSchema(SystemSchema.Name, SchemaTypes.System);
			CreateSchema(InformationSchema.SchemaName, SchemaTypes.System);
			CreateSchema(query.Session.Database().Context.DefaultSchema(), SchemaTypes.Default);
		}
	}
}
