using System;
using Deveel.Data.Sql.Schemata;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Constraints {
	class ConstraintsSystemFeature : ISystemFeature {
		public string Name => "constraints";

		public Version Version => typeof(ConstraintsSystemFeature).Assembly.GetName().Version;

		public void OnSystemCreate(ISession session) {

			// TODO: must implement and test the session.CreateTable call first

			//// SYSTEM.PKEY_INFO
			//var tableInfo = new TableInfo(SystemSchema.PrimaryKeyInfoTableName);
			//tableInfo.Columns.Add("id", PrimitiveTypes.Integer())
			//	.Add("name", PrimitiveTypes.VarChar(256))
			//	.Add("schema", PrimitiveTypes.VarChar(256))
			//	.Add("table", PrimitiveTypes.VarChar(256))
			//	.Add("deferred", PrimitiveTypes.TinyInt());

			//session.CreateTable(tableInfo);


			//// SYSTEM.PKEY_COLS
			//tableInfo = new TableInfo(SystemSchema.PrimaryKeyColumnsTableName);
			//tableInfo.Columns.Add("pk_id", PrimitiveTypes.Integer())
			//	.Add("column", PrimitiveTypes.VarChar(256))
			//	.Add("seq_no", PrimitiveTypes.Integer());

			//session.CreateTable(tableInfo);

			//// SYSTEM.FKEY_INFO
			//tableInfo = new TableInfo(SystemSchema.ForeignKeyInfoTableName);
			//tableInfo.Columns.Add("id", PrimitiveTypes.Integer())
			//	.Add("name", PrimitiveTypes.VarChar(256))
			//	.Add("schema", PrimitiveTypes.VarChar(256))
			//	.Add("table", PrimitiveTypes.VarChar(256))
			//	.Add("ref_schema", PrimitiveTypes.VarChar(256))
			//	.Add("ref_table", PrimitiveTypes.VarChar(256))
			//	.Add("update_rule", PrimitiveTypes.TinyInt())
			//	.Add("delete_rule", PrimitiveTypes.TinyInt())
			//	.Add("deferred", PrimitiveTypes.TinyInt());

			//session.CreateTable(tableInfo);

			//// SYSTEM.FKEY_COLS
			//tableInfo = new TableInfo(SystemSchema.ForeignKeyColumnsTableName);
			//tableInfo.Columns.Add("fk_id", PrimitiveTypes.Integer())
			//	.Add("fcolumn", PrimitiveTypes.VarChar(256))
			//	.Add("pcolumn", PrimitiveTypes.VarChar(256))
			//	.Add("seq_no", PrimitiveTypes.Integer());

			//session.CreateTable(tableInfo);

			//// SYSTEM.UNIQUE_INFO
			//tableInfo = new TableInfo(SystemSchema.UniqueKeyInfoTableName);
			//tableInfo.Columns
			//	.Add("id", PrimitiveTypes.Integer())
			//	.Add("name", PrimitiveTypes.VarChar(256))
			//	.Add("schema", PrimitiveTypes.VarChar(256))
			//	.Add("table", PrimitiveTypes.VarChar(256))
			//	.Add("deferred", PrimitiveTypes.TinyInt());

			//session.CreateTable(tableInfo);

			//// SYSTEM.UNIQUE_COLS
			//tableInfo = new TableInfo(SystemSchema.UniqueKeyColumnsTableName);
			//tableInfo.Columns.Add("un_id", PrimitiveTypes.Integer())
			//	.Add("column", PrimitiveTypes.VarChar(256))
			//	.Add("seq_no", PrimitiveTypes.Integer());

			//session.CreateTable(tableInfo);

			//// SYSTEM.CHECK_INFO
			//tableInfo = new TableInfo(SystemSchema.CheckInfoTableName);
			//tableInfo.Columns
			//	.Add("id", PrimitiveTypes.Integer())
			//	.Add("name", PrimitiveTypes.VarChar(256))
			//	.Add("schema", PrimitiveTypes.VarChar(256))
			//	.Add("table", PrimitiveTypes.VarChar(256))
			//	.Add("expression", PrimitiveTypes.String())
			//	.Add("deferred", PrimitiveTypes.TinyInt());

			//session.CreateTable(tableInfo);
		}

		public void OnSystemSetup(ISession session) {
			throw new NotImplementedException();
		}
	}
}