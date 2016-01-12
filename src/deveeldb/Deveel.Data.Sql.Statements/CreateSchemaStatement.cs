using System;

using Deveel.Data.Security;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Schemas;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class CreateSchemaStatement : SqlStatement {
		public CreateSchemaStatement(string schemaName) {
			if (String.IsNullOrEmpty(schemaName))
				throw new ArgumentNullException("schemaName");
			if (String.Equals(SystemSchema.Name, schemaName, StringComparison.OrdinalIgnoreCase) ||
				String.Equals(InformationSchema.SchemaName, schemaName, StringComparison.OrdinalIgnoreCase))
				throw new ArgumentException(String.Format("The schema name '{0}' is reserved.", schemaName));

			SchemaName = schemaName;
		}

		private CreateSchemaStatement(ObjectData data) {
			SchemaName = data.GetString("SchemaName");
		}

		public string SchemaName { get; private set; }

		protected override void GetData(SerializeData data) {
			data.SetValue("SchemaName", SchemaName);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.Query.UserCanCreateSchema())
				throw new MissingPrivilegesException(context.User.Name, new ObjectName(SchemaName), Privileges.Create);

			if (context.Query.SchemaExists(SchemaName))
				throw new InvalidOperationException(String.Format("The schema '{0}' already exists in the system.", SchemaName));

			context.Query.CreateSchema(SchemaName, SchemaTypes.User);

			// TODO: Grant to the current user all privileges on the schema
		}
	}
}
