using System;

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
	class RoutinesSystemCreateCallback : ISystemCreateCallback {
		private readonly IQuery query;

		public RoutinesSystemCreateCallback(IQuery query) {
			this.query = query;
		}

		void ISystemCreateCallback.Activate(SystemCreatePhase phase) {
			if (phase == SystemCreatePhase.SystemSetup) {
				Create();
				AddForeignKeys();
			} else if (phase == SystemCreatePhase.DatabaseCreate) {
				GrantToPublic();
			}
		}

		private void Create() {
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
			query.Access().CreateTable(tableInfo);

			// SYSTEM.ROUTINE_PARAM
			tableInfo = new TableInfo(RoutineManager.RoutineParameterTableName);
			tableInfo.AddColumn("routine_id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("arg_name", PrimitiveTypes.String());
			tableInfo.AddColumn("arg_type", PrimitiveTypes.String());
			tableInfo.AddColumn("arg_attrs", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("in_out", PrimitiveTypes.Integer());
			tableInfo.AddColumn("offset", PrimitiveTypes.Integer());
			query.Access().CreateTable(tableInfo);
		}

		private void AddForeignKeys() {
			var fkCol = new[] { "routine_id" };
			var refCol = new[] { "id" };
			const ForeignKeyAction onUpdate = ForeignKeyAction.NoAction;
			const ForeignKeyAction onDelete = ForeignKeyAction.Cascade;

			query.Access().AddForeignKey(RoutineManager.RoutineParameterTableName, fkCol, RoutineManager.RoutineTableName, refCol,
				onDelete, onUpdate, "ROUTINE_PARAMS_FK");
		}

		private void GrantToPublic() {
			query.Access().GrantOnTable(RoutineManager.RoutineTableName, User.PublicName, Privileges.TableRead);
			query.Access().GrantOnTable(RoutineManager.RoutineParameterTableName, User.PublicName, Privileges.TableRead);
		}
	}
}
