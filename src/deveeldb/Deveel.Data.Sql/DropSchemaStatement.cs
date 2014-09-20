// 
//  Copyright 2010-2014 Deveel
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

using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Security;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class DropSchemaStatement : Statement {
		public DropSchemaStatement(string schemaName) {
			SchemaName = schemaName;
		}

		public DropSchemaStatement() {	
		}

		public string SchemaName {
			get { return GetString("schema_name"); }
			set {
				if (String.IsNullOrEmpty(value))
					throw new ArgumentNullException("value");
				SetValue("schema_name", value);
			}
		}

		protected override Table Evaluate(IQueryContext context) {
			string schemaName = GetString("schema_name");

			if (!context.Connection.Database.CanUserCreateAndDropSchema(context, schemaName))
				throw new UserAccessException("User not permitted to create or drop schema.");

			SchemaDef schema = ResolveSchemaName(context, schemaName);
			// Only allow user to drop USER typed schemas
			if (schema == null)
				throw new DatabaseException("Schema '" + schemaName + "' does not exist.");

			if (!schema.Type.Equals("USER"))
				throw new DatabaseException("Can not drop schema '" + schemaName + "'");

			// Check if the schema is empty.
			TableName[] allTables = context.Connection.GetTables();
			string resolvedSchemaName = schema.Name;
			foreach (TableName tableName in allTables) {
				if (tableName.Schema.Equals(resolvedSchemaName))
					throw new DatabaseException("Schema '" + schemaName + "' is not empty.");
			}

			// Drop the schema
			context.Connection.DropSchema(schema.Name);

			// Revoke all the grants for the schema
			context.Connection.GrantManager.RevokeAllGrantsOnObject(GrantObject.Schema, schema.Name);

			return FunctionTable.ResultTable(context, 0);
		}
	}
}