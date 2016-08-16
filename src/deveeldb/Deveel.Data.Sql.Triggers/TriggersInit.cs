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
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Triggers {
	class TriggersInit : ITableCompositeCreateCallback {
		public void OnTableCompositeCreate(IQuery systemQuery) {
			var tableInfo = new TableInfo(TriggerManager.TriggerTableName);
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.Integer());
			tableInfo.AddColumn("on_object", PrimitiveTypes.String());
			tableInfo.AddColumn("time", PrimitiveTypes.Integer());
			tableInfo.AddColumn("action", PrimitiveTypes.Integer());
			tableInfo.AddColumn("procedure_name", PrimitiveTypes.String());
			tableInfo.AddColumn("args", PrimitiveTypes.Binary());
			tableInfo.AddColumn("body", PrimitiveTypes.Binary());
			tableInfo.AddColumn("status", PrimitiveTypes.TinyInt());
			systemQuery.Access().CreateTable(tableInfo);
		}
	}
}
