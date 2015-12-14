using System;

namespace Deveel.Data.Sql.Statements {
	public sealed class DropSchemaStatement : SqlPreparedStatement {
		public DropSchemaStatement(string schemaName) {
			if (String.IsNullOrEmpty(schemaName))
				throw new ArgumentNullException("schemaName");

			SchemaName = schemaName;
		}

		public string SchemaName { get; private set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			throw new NotImplementedException();
		}
	}
}
