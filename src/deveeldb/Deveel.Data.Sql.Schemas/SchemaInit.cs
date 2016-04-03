using System;

using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Schemas {
	class SchemaInit : ITableCompositeCreateCallback {
		public void OnTableCompositeCreate(IQuery systemQuery) {
			// SYSTEM.SCHEMA_INFO
			var tableInfo = new TableInfo(SystemSchema.SchemaInfoTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.String());
			tableInfo.AddColumn("culture", PrimitiveTypes.String());
			tableInfo.AddColumn("other", PrimitiveTypes.String());
			tableInfo = tableInfo.AsReadOnly();
			systemQuery.Access().CreateTable(tableInfo);

			// TODO: Move this to the setup phase?
			CreateSystemSchema(systemQuery);
		}

		private void CreateSchema(IQuery systemQuery, string name, string type) {
			systemQuery.Access().CreateSchema(new SchemaInfo(name, type));
		}

		private void CreateSystemSchema(IQuery systemQuery) {
			CreateSchema(systemQuery, SystemSchema.Name, SchemaTypes.System);
			CreateSchema(systemQuery, InformationSchema.SchemaName, SchemaTypes.System);
			CreateSchema(systemQuery, systemQuery.Session.Database().Context.DefaultSchema(), SchemaTypes.Default);
		}
	}
}
