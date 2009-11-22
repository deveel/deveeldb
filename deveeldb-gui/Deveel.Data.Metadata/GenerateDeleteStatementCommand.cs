using System;

using Deveel.Data.Commands;
using Deveel.Data.DbModel;

namespace Deveel.Data.Metadata {
	[Command("GenerateDelete", "Generate Delete Statement")]
	[CommandImage("Deveel.Data.Images.table_row_delete.png")]
	public class GenerateDeleteStatementCommand : GenerateStatementCommandBase {
		public override void Execute() {
			IQueryEditor editor = QueryEditor;
			string tableName = HostWindow.MetadataProvider.SelectedTable;
			DbSchema schema = HostWindow.MetadataProvider.Schema;

			if (tableName != null && editor != null) {
				DbObject dbObject = schema[tableName];
				string sql = null;
				if (dbObject is DbTable)
					sql = Formatter.FormatDelete((DbTable) dbObject, DbTableValues.FromPrimaryKey(dbObject as DbTable));
				if (sql != null)
					editor.Insert(sql);
			}
		}
	}
}