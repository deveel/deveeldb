// 
//  Copyright 2010-2016 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


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
			tableInfo.AddColumn("abstract", PrimitiveTypes.Boolean());
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
