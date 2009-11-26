using System;

namespace Deveel.Data.Sql {
	public sealed class OpenCursorStatement : Statement {
		private string name;

		private TableName resolved_name;

		internal override void Prepare() {
			DatabaseConnection db = Connection;

			name = GetString("name");

			string schema_name = db.CurrentSchema;
			resolved_name = TableName.Resolve(schema_name, name);

			string name_strip = resolved_name.Name;

			if (name_strip.IndexOf('.') != -1)
				throw new DatabaseException("Cursor name can not contain '.' character.");
		}

		internal override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);
			Cursor cursor = Connection.GetCursor(resolved_name);
			if (cursor == null)
				throw new InvalidOperationException("The cursor '" + name + "' was not defined within this transaction.");

			cursor.Open(context);
			return FunctionTable.ResultTable(context, 1);
		}
	}
}