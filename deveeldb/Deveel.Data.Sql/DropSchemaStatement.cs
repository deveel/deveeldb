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
	public sealed class DropSchemaStatement : Statement {
		/// <summary>
		/// The name of the schema.
		/// </summary>
		private String schema_name;

		protected override void Prepare() {
			schema_name = GetString("schema_name");
		}

		protected override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			if (!Connection.Database.CanUserCreateAndDropSchema(context, User, schema_name))
				throw new UserAccessException("User not permitted to create or drop schema.");

			bool ignore_case = Connection.IsInCaseInsensitiveMode;
			SchemaDef schema =
						Connection.ResolveSchemaCase(schema_name, ignore_case);
			// Only allow user to drop USER typed schemas
			if (schema == null) {
				throw new DatabaseException("Schema '" + schema_name + "' does not exist.");
			} else if (schema.Type.Equals("USER")) {
				// Check if the schema is empty.
				TableName[] all_tables = Connection.Tables;
				String resolved_schema_name = schema.Name;
				for (int i = 0; i < all_tables.Length; ++i) {
					if (all_tables[i].Schema.Equals(resolved_schema_name)) {
						throw new DatabaseException("Schema '" + schema_name + "' is not empty.");
					}
				}
				// Drop the schema
				Connection.DropSchema(schema.Name);
				// Revoke all the grants for the schema
				Connection.GrantManager.RevokeAllGrantsOnObject(GrantObject.Schema, schema.Name);

			} else {
				throw new DatabaseException("Can not drop schema '" + schema_name + "'");
			}

			return FunctionTable.ResultTable(context, 0);
		}
	}
}