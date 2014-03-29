// 
//  Copyright 2010  Deveel
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
	public sealed class CreateSchemaStatement : Statement {
		public CreateSchemaStatement(string schemaName) {
			SchemaName = schemaName;
		}

		public CreateSchemaStatement() {
		}

		/// <summary>
		/// The name of the schema.
		/// </summary>
		private string schemaName;

		public string SchemaName {
			get { return GetString("schema_name"); }
			set {
				if (String.IsNullOrEmpty(value))
					throw new ArgumentNullException("value");
				SetValue("schema_name", value);
			}
		}

		protected override void Prepare(IQueryContext context) {
			schemaName = GetString("schema_name");
		}

		protected override Table Evaluate(IQueryContext context) {
			if (!context.Connection.Database.CanUserCreateAndDropSchema(context, schemaName))
				throw new UserAccessException("User not permitted to create or drop schema.");

			SchemaDef schema = ResolveSchemaName(context, schemaName);
			if (schema != null)
				throw new DatabaseException("Schema '" + schemaName + "' already exists.");

			// Create the schema
			context.Connection.CreateSchema(schemaName, "USER");
			// Set the default grants for the schema
			context.Connection.GrantManager.Grant(Privileges.SchemaAll,
			                              GrantObject.Schema, schemaName, context.UserName,
			                              true, Database.InternalSecureUsername);

			return FunctionTable.ResultTable(context, 0);
		}
	}
}