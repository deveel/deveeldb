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

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
	class RoutinesInit : ITableCompositeSetupCallback, IDatabaseCreateCallback {
		private void Create(IQuery systemQuery) {
			// SYSTEM.ROUTINE
			var tableInfo = new TableInfo(RoutineManager.RoutineTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.String());
			tableInfo.AddColumn("location", PrimitiveTypes.String());
			tableInfo.AddColumn("body", PrimitiveTypes.Binary());
			tableInfo.AddColumn("return_type", PrimitiveTypes.String());
			tableInfo.AddColumn("username", PrimitiveTypes.String());
			systemQuery.Access().CreateTable(tableInfo);

			// SYSTEM.ROUTINE_PARAM
			tableInfo = new TableInfo(RoutineManager.RoutineParameterTableName);
			tableInfo.AddColumn("routine_id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("arg_name", PrimitiveTypes.String());
			tableInfo.AddColumn("arg_type", PrimitiveTypes.String());
			tableInfo.AddColumn("arg_attrs", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("in_out", PrimitiveTypes.Integer());
			tableInfo.AddColumn("offset", PrimitiveTypes.Integer());
			systemQuery.Access().CreateTable(tableInfo);
		}

		private void AddForeignKeys(IQuery systemQuery) {
			var fkCol = new[] { "routine_id" };
			var refCol = new[] { "id" };
			const ForeignKeyAction onUpdate = ForeignKeyAction.NoAction;
			const ForeignKeyAction onDelete = ForeignKeyAction.Cascade;

			systemQuery.Access().AddForeignKey(RoutineManager.RoutineParameterTableName, fkCol, RoutineManager.RoutineTableName, refCol,
				onDelete, onUpdate, "ROUTINE_PARAMS_FK");
		}

		private void GrantToPublic(IQuery systemQuery) {
			systemQuery.Access().GrantOnTable(RoutineManager.RoutineTableName, User.PublicName, Privileges.TableRead);
			systemQuery.Access().GrantOnTable(RoutineManager.RoutineParameterTableName, User.PublicName, Privileges.TableRead);
		}

		public void OnTableCompositeSetup(IQuery systemQuery) {
			Create(systemQuery);
			AddForeignKeys(systemQuery);
		}

		public void OnDatabaseCreate(IQuery systemQuery) {
			GrantToPublic(systemQuery);
		}
	}
}
