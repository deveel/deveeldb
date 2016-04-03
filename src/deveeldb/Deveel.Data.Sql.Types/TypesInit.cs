using System;

using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Types {
	class TypesInit : ITableCompositeCreateCallback {
		public void OnTableCompositeCreate(IQuery systemQuery) {
			var tableInfo = new TableInfo(TypeManager.TypeTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("schema", PrimitiveTypes.String(), true);
			tableInfo.AddColumn("name", PrimitiveTypes.String(), true);
			tableInfo.AddColumn("parent", PrimitiveTypes.String());
			tableInfo.AddColumn("sealed", PrimitiveTypes.Boolean());
			tableInfo.AddColumn("owner", PrimitiveTypes.String());
			systemQuery.Access().CreateTable(tableInfo);

			tableInfo = new TableInfo(TypeManager.TypeMemberTableName);
			tableInfo.AddColumn("type_id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("name", PrimitiveTypes.String(), true);
			tableInfo.AddColumn("type", PrimitiveTypes.String());
			systemQuery.Access().CreateTable(tableInfo);

			systemQuery.Access().AddPrimaryKey(TypeManager.TypeTableName, new[] { "id" }, "PK_TYPE");
			systemQuery.Access().AddForeignKey(TypeManager.TypeMemberTableName, new[] { "type_id" }, TypeManager.TypeTableName, new[] { "id" }, ForeignKeyAction.Cascade, ForeignKeyAction.Cascade, "FK_MEMBER_TYPE");
		}
	}
}
