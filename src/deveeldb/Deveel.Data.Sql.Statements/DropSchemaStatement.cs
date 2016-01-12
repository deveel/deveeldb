using System;

using Deveel.Data.Security;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Schemas;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropSchemaStatement : SqlStatement {
		public DropSchemaStatement(string schemaName) {
			if (String.IsNullOrEmpty(schemaName))
				throw new ArgumentNullException("schemaName");
			if (String.Equals(InformationSchema.SchemaName, schemaName, StringComparison.OrdinalIgnoreCase) ||
				String.Equals(SystemSchema.Name, schemaName, StringComparison.OrdinalIgnoreCase))
				throw new ArgumentException(String.Format("The schema name '{0}' is reserved and cannot be dropped.", schemaName));

			SchemaName = schemaName;
		}

		private DropSchemaStatement(ObjectData data) {
			SchemaName = data.GetString("SchemaName");
		}

		public string SchemaName { get; private set; }

		protected override void GetData(SerializeData data) {
			data.SetValue("SchemaName", SchemaName);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.Query.SchemaExists(SchemaName))
				throw new InvalidOperationException(String.Format("The schema '{0}' does not exist.", SchemaName));

			if (!context.Query.UserCanDropSchema(SchemaName))
				throw new MissingPrivilegesException(context.User.Name, new ObjectName(SchemaName), Privileges.Drop);

			// TODO: Check if the schema is empty before deleting it

			context.Query.DropSchema(SchemaName);

			// TODO: Remove all the grants on this schema...
		}
	}
}
