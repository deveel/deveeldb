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

namespace Deveel.Data.Sql {
	public sealed class CreateSchemaStatement : Statement {
		public CreateSchemaStatement(string schemaName) {
			SchemaName = schemaName;
		}

		public CreateSchemaStatement() {
		}

		/// <summary>
		/// The name of the schema.
		/// </summary>
		private string schema_name;

		public string SchemaName {
			get { return GetString("schema_name"); }
			set {
				if (String.IsNullOrEmpty(value))
					throw new ArgumentNullException("value");
				SetValue("schema_name", value);
			}
		}

		protected override void Prepare() {
			schema_name = GetString("schema_name");
		}

		protected override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			if (!Connection.Database.CanUserCreateAndDropSchema(context, User, schema_name))
				throw new UserAccessException("User not permitted to create or drop schema.");

			bool ignore_case = Connection.IsInCaseInsensitiveMode;
			SchemaDef schema = Connection.ResolveSchemaCase(schema_name, ignore_case);
			if (schema == null) {
				// Create the schema
				Connection.CreateSchema(schema_name, "USER");
				// Set the default grants for the schema
				Connection.GrantManager.Grant(Privileges.SchemaAll,
							GrantObject.Schema, schema_name, User.UserName,
							true, Database.InternalSecureUsername);
			} else {
				throw new DatabaseException("Schema '" + schema_name + "' already exists.");
			}

			return FunctionTable.ResultTable(context, 0);
		}
	}
}